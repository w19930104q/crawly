defmodule Crawly.BatchCommitter do
  @moduledoc """
  Idempotent commit boundary for parsed batches.

  When a response is parsed, the derived requests and items form a single
  batch. This module makes sure the batch either becomes visible in
  `Crawly.RequestsStorage` and `Crawly.DataStorage` as a whole, or not at
  all:

    * A batch is identified by `{spider_name, crawl_id, batch_id}`, where
      `batch_id` is derived from the response and the attempt number by
      `Crawly.Worker`. Re-delivering the same response for the same attempt
      is reported as `:duplicate` and never re-enqueues requests or
      re-produces items.
    * Before anything is stored, both storages are validated (workers are
      running, the requests queue has enough capacity — see the
      `:requests_storage_max_size` setting). If validation fails, nothing
      is stored and the batch is reported as `{:failed, reason}`, so a
      failure never leaves a half-committed batch behind.
    * Items are stored through `Crawly.DataStorage.store_batch/2`, which
      runs item pipelines strictly: a crashing pipeline aborts the whole
      batch and leaves the storage state untouched.
    * A failed batch is *not* remembered, so it can be committed again
      once the failure is fixed. Only successfully committed batches are
      deduplicated.

  Statistics per `{spider_name, crawl_id}` distinguish `:accepted`,
  `:duplicate` and `:failed` batches. See `stats/2`.
  """
  use GenServer

  require Logger

  defstruct committed: MapSet.new(), stats: %{}

  @empty_stats %{accepted: 0, duplicate: 0, failed: 0}

  @type commit_result :: :accepted | :duplicate | {:failed, term()}

  def start_link([]) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Atomically commit a batch of `requests` and `items` produced by parsing
  a response.

  `batch_id` must uniquely identify the response and the attempt which
  produced the batch. Returns:

    * `:accepted` — the whole batch was stored and is now visible;
    * `:duplicate` — this batch was already committed before, nothing was
      stored;
    * `{:failed, reason}` — the batch could not be committed, nothing was
      stored.
  """
  @spec commit(
          spider_name :: atom(),
          crawl_id :: term(),
          batch_id :: term(),
          requests :: [Crawly.Request.t()],
          items :: [map()]
        ) :: commit_result()
  def commit(spider_name, crawl_id, batch_id, requests, items) do
    GenServer.call(
      __MODULE__,
      {:commit, spider_name, crawl_id, batch_id, requests, items}
    )
  end

  @doc """
  Return commit statistics for a given `{spider_name, crawl_id}` pair.

  The result is a map with `:accepted`, `:duplicate` and `:failed`
  counters.
  """
  @spec stats(spider_name :: atom(), crawl_id :: term()) :: %{
          accepted: non_neg_integer(),
          duplicate: non_neg_integer(),
          failed: non_neg_integer()
        }
  def stats(spider_name, crawl_id) do
    GenServer.call(__MODULE__, {:stats, spider_name, crawl_id})
  end

  @doc false
  def reset(), do: GenServer.call(__MODULE__, :reset)

  @impl true
  def init([]), do: {:ok, %__MODULE__{}}

  @impl true
  def handle_call(
        {:commit, spider_name, crawl_id, batch_id, requests, items},
        _from,
        state
      ) do
    key = {spider_name, crawl_id, batch_id}

    if MapSet.member?(state.committed, key) do
      {:reply, :duplicate, bump_stat(state, spider_name, crawl_id, :duplicate)}
    else
      case do_commit(spider_name, requests, items) do
        :ok ->
          new_state =
            state
            |> Map.put(:committed, MapSet.put(state.committed, key))
            |> bump_stat(spider_name, crawl_id, :accepted)

          {:reply, :accepted, new_state}

        {:error, reason} ->
          Logger.warning(
            "Batch commit failed for #{inspect(spider_name)}, " <>
              "reason: #{inspect(reason)}"
          )

          {:reply, {:failed, reason},
           bump_stat(state, spider_name, crawl_id, :failed)}
      end
    end
  end

  def handle_call({:stats, spider_name, crawl_id}, _from, state) do
    stats = Map.get(state.stats, {spider_name, crawl_id}, @empty_stats)
    {:reply, stats, state}
  end

  def handle_call(:reset, _from, _state) do
    {:reply, :ok, %__MODULE__{}}
  end

  # Validate both storages first, then store items and requests. Items go
  # first: their storage is the one which can still fail at commit time
  # (a crashing item pipeline), and at that point no request has been
  # enqueued yet, so a failure leaves no half batch behind.
  #
  # Storage failures (including storage processes dying while a crawl is
  # being torn down) are turned into `{:error, reason}` tuples, so the
  # committer itself never crashes and never loses its committed batches.
  defp do_commit(spider_name, requests, items) do
    with :ok <- prepare_requests_storage(spider_name, requests),
         :ok <- prepare_data_storage(spider_name, items),
         :ok <- store_items(spider_name, items),
         :ok <- store_requests(spider_name, requests) do
      :ok
    else
      {:error, reason} -> {:error, reason}
      other -> {:error, other}
    end
  catch
    error, reason ->
      Logger.warning(
        "Batch commit for #{inspect(spider_name)} failed with " <>
          "#{inspect(error)}: #{inspect(reason)}"
      )

      {:error, {error, reason}}
  end

  defp prepare_requests_storage(_spider_name, []), do: :ok

  defp prepare_requests_storage(spider_name, requests),
    do: Crawly.RequestsStorage.prepare(spider_name, length(requests))

  defp prepare_data_storage(_spider_name, []), do: :ok

  defp prepare_data_storage(spider_name, _items),
    do: Crawly.DataStorage.prepare(spider_name)

  defp store_items(_spider_name, []), do: :ok

  defp store_items(spider_name, items),
    do: Crawly.DataStorage.store_batch(spider_name, items)

  defp store_requests(_spider_name, []), do: :ok

  defp store_requests(spider_name, requests),
    do: Crawly.RequestsStorage.store_batch(spider_name, requests)

  defp bump_stat(state, spider_name, crawl_id, field) do
    key = {spider_name, crawl_id}

    new_stats =
      state.stats
      |> Map.get(key, @empty_stats)
      |> Map.update!(field, &(&1 + 1))

    %{state | stats: Map.put(state.stats, key, new_stats)}
  end
end
