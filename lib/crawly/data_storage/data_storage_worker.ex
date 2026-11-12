defmodule Crawly.DataStorage.Worker do
  @moduledoc """
  A worker process which stores items for individual spiders. All items
  are pre-processed by item_pipelines.

  All pipelines are using the state of this process for their internal needs
  (persistancy).

  The DataStorage.Worker will not write anything to a filesystem. Instead it
  would expect that pipelines are going to do that work.
  """
  alias Crawly.DataStorage.Worker
  require Logger

  use GenServer

  defstruct stored_items: 0, spider_name: nil, crawl_id: nil

  def start_link(spider_name: spider_name, crawl_id: crawl_id) do
    GenServer.start_link(__MODULE__,
      spider_name: spider_name,
      crawl_id: crawl_id
    )
  end

  @spec stats(pid()) :: {:stored_items, non_neg_integer()}
  def stats(pid), do: GenServer.call(pid, :stats)

  @spec store(pid(), map()) :: :ok
  def store(pid, item) do
    GenServer.cast(pid, {:store, item})
  end

  @doc """
  Store a batch of items atomically.

  Items are piped through the item pipelines in a strict mode: a crashing
  pipeline aborts the whole batch with an `{:error, reason}` tuple and the
  worker state is left untouched. When all items are piped successfully,
  the new state (including the stored items count) becomes visible at
  once.
  """
  @spec store_batch(pid(), [map()]) :: :ok | {:error, term()}
  def store_batch(pid, items) do
    GenServer.call(pid, {:store_batch, items})
  catch
    error, reason ->
      Logger.error(Exception.format(error, reason, __STACKTRACE__))
      {:error, {error, reason}}
  end

  @doc """
  Inspect the inner state of the given data worker
  """
  @spec inspect(pid, atom()) :: term()
  def inspect(pid, field) do
    GenServer.call(pid, {:inspect, field})
  end

  def init(spider_name: spider_name, crawl_id: crawl_id) do
    Logger.metadata(spider_name: spider_name, crawl_id: crawl_id)
    {:ok, %Worker{spider_name: spider_name, crawl_id: crawl_id}}
  end

  def handle_cast({:store, item}, state) do
    pipelines = Crawly.Utils.get_settings(:pipelines, state.spider_name, [])

    state =
      case Crawly.Utils.pipe(pipelines, item, state) do
        {false, new_state} ->
          new_state

        {new_item, new_state} ->
          Logger.debug("Stored item: #{inspect(new_item)}")
          %Worker{new_state | stored_items: state.stored_items + 1}
      end

    {:noreply, state}
  end

  def handle_call({:store_batch, items}, _from, state) do
    pipelines = Crawly.Utils.get_settings(:pipelines, state.spider_name, [])

    case pipe_batch(pipelines, items, state) do
      {:ok, new_state, stored} ->
        {:reply, :ok,
         %Worker{new_state | stored_items: state.stored_items + stored}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:inspect, field}, _from, state) do
    msg = {:inspect, Map.get(state, field, nil)}
    {:reply, msg, state}
  end

  def handle_call(:stats, _from, state) do
    {:reply, {:stored_items, state.stored_items}, state}
  end

  # Pipe the whole batch of items through the pipelines strictly: a
  # crashing pipeline aborts the batch, so the caller can drop the batch
  # without making any part of it visible.
  defp pipe_batch(pipelines, items, state) do
    Enum.reduce_while(items, {:ok, state, 0}, fn item,
                                                 {:ok, acc_state, stored} ->
      try do
        case Crawly.Utils.strict_pipe(pipelines, item, acc_state) do
          {false, new_state} ->
            {:cont, {:ok, new_state, stored}}

          {new_item, new_state} ->
            Logger.debug("Stored item: #{inspect(new_item)}")
            {:cont, {:ok, new_state, stored + 1}}
        end
      catch
        error, reason ->
          Logger.error(
            "Pipeline crash while storing a batch, the batch is aborted: " <>
              Exception.format(error, reason, __STACKTRACE__)
          )

          {:halt, {:error, {error, reason}}}
      end
    end)
  end
end
