defmodule BatchCommitterTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  defmodule CrashingPipeline do
    @behaviour Crawly.Pipeline

    @impl Crawly.Pipeline
    def run(_item, _state), do: raise("pipeline boom")
  end

  defmodule BatchTestSpider do
    use Crawly.Spider

    @impl Crawly.Spider
    def base_url() do
      "https://www.example.com"
    end

    @impl Crawly.Spider
    def init() do
      [start_urls: ["https://www.example.com"]]
    end

    @impl Crawly.Spider
    def parse_item(_response) do
      %Crawly.ParsedItem{
        items: [%{id: 1}],
        requests: [
          Crawly.Request.new("https://www.example.com/next", [], [], [])
        ]
      }
    end
  end

  setup do
    spider_name =
      String.to_atom("batch_test_spider_#{System.unique_integer([:positive])}")

    crawl_id = "crawl_#{System.unique_integer([:positive])}"

    {:ok, requests_storage_pid} =
      Crawly.RequestsStorage.start_worker(spider_name, crawl_id)

    {:ok, data_storage_pid} =
      Crawly.DataStorage.start_worker(spider_name, crawl_id)

    prev_pipelines = Application.get_env(:crawly, :pipelines)
    prev_max_size = Application.get_env(:crawly, :requests_storage_max_size)
    Application.put_env(:crawly, :pipelines, [])
    Application.delete_env(:crawly, :requests_storage_max_size)

    on_exit(fn ->
      restore_env(:pipelines, prev_pipelines)
      restore_env(:requests_storage_max_size, prev_max_size)
      :ok = TestUtils.stop_process(requests_storage_pid)
      :ok = TestUtils.stop_process(data_storage_pid)
      wait_for_deregistration(Crawly.RequestsStorage, spider_name)
      wait_for_deregistration(Crawly.DataStorage, spider_name)
    end)

    {:ok, spider_name: spider_name, crawl_id: crawl_id}
  end

  describe "batch commit" do
    test "accepted batch makes all requests and items visible", context do
      requests = [req("/1"), req("/2")]
      items = [%{id: 1}, %{id: 2}]

      assert :accepted =
               Crawly.BatchCommitter.commit(
                 context.spider_name,
                 context.crawl_id,
                 "batch_1",
                 requests,
                 items
               )

      assert {:stored_requests, 2} =
               Crawly.RequestsStorage.stats(context.spider_name)

      assert {:stored_items, 2} = Crawly.DataStorage.stats(context.spider_name)

      assert %{accepted: 1, duplicate: 0, failed: 0} =
               Crawly.BatchCommitter.stats(
                 context.spider_name,
                 context.crawl_id
               )
    end

    test "replayed batch is a duplicate: no re-enqueue, no re-produced items",
         context do
      requests = [req("/1"), req("/2")]
      items = [%{id: 1}, %{id: 2}]

      assert :accepted =
               Crawly.BatchCommitter.commit(
                 context.spider_name,
                 context.crawl_id,
                 "batch_1",
                 requests,
                 items
               )

      assert :duplicate =
               Crawly.BatchCommitter.commit(
                 context.spider_name,
                 context.crawl_id,
                 "batch_1",
                 requests,
                 items
               )

      assert {:stored_requests, 2} =
               Crawly.RequestsStorage.stats(context.spider_name)

      assert {:stored_items, 2} = Crawly.DataStorage.stats(context.spider_name)

      assert %{accepted: 1, duplicate: 1, failed: 0} =
               Crawly.BatchCommitter.stats(
                 context.spider_name,
                 context.crawl_id
               )
    end

    test "batches are isolated by crawl_id", context do
      requests = [req("/1")]
      items = [%{id: 1}]

      assert :accepted =
               Crawly.BatchCommitter.commit(
                 context.spider_name,
                 "crawl_A",
                 "batch_1",
                 requests,
                 items
               )

      # The same batch under another crawl_id is a different batch
      assert :accepted =
               Crawly.BatchCommitter.commit(
                 context.spider_name,
                 "crawl_B",
                 "batch_1",
                 requests,
                 items
               )

      assert :duplicate =
               Crawly.BatchCommitter.commit(
                 context.spider_name,
                 "crawl_A",
                 "batch_1",
                 requests,
                 items
               )

      assert %{accepted: 1, duplicate: 1, failed: 0} =
               Crawly.BatchCommitter.stats(context.spider_name, "crawl_A")

      assert %{accepted: 1, duplicate: 0, failed: 0} =
               Crawly.BatchCommitter.stats(context.spider_name, "crawl_B")

      assert {:stored_requests, 2} =
               Crawly.RequestsStorage.stats(context.spider_name)

      assert {:stored_items, 2} = Crawly.DataStorage.stats(context.spider_name)
    end
  end

  describe "half batch failures" do
    test "data storage not running: no requests are enqueued", context do
      spider_name =
        String.to_atom("no_ds_spider_#{System.unique_integer([:positive])}")

      {:ok, requests_storage_pid} =
        Crawly.RequestsStorage.start_worker(spider_name, context.crawl_id)

      on_exit(fn ->
        :ok = TestUtils.stop_process(requests_storage_pid)
        wait_for_deregistration(Crawly.RequestsStorage, spider_name)
      end)

      assert {:failed, :data_storage_worker_not_running} =
               Crawly.BatchCommitter.commit(
                 spider_name,
                 context.crawl_id,
                 "batch_1",
                 [req("/1")],
                 [%{id: 1}]
               )

      # Nothing leaked into the requests storage
      assert {:stored_requests, 0} = Crawly.RequestsStorage.stats(spider_name)

      assert %{accepted: 0, duplicate: 0, failed: 1} =
               Crawly.BatchCommitter.stats(spider_name, context.crawl_id)

      # A failed batch is not remembered: once the storage is fixed, the
      # very same batch can be committed.
      {:ok, data_storage_pid} =
        Crawly.DataStorage.start_worker(spider_name, context.crawl_id)

      on_exit(fn ->
        :ok = TestUtils.stop_process(data_storage_pid)
        wait_for_deregistration(Crawly.DataStorage, spider_name)
      end)

      assert :accepted =
               Crawly.BatchCommitter.commit(
                 spider_name,
                 context.crawl_id,
                 "batch_1",
                 [req("/1")],
                 [%{id: 1}]
               )

      assert {:stored_requests, 1} = Crawly.RequestsStorage.stats(spider_name)
      assert {:stored_items, 1} = Crawly.DataStorage.stats(spider_name)
    end

    test "requests storage not running: no items are stored", context do
      spider_name =
        String.to_atom("no_rs_spider_#{System.unique_integer([:positive])}")

      {:ok, data_storage_pid} =
        Crawly.DataStorage.start_worker(spider_name, context.crawl_id)

      on_exit(fn ->
        :ok = TestUtils.stop_process(data_storage_pid)
        wait_for_deregistration(Crawly.DataStorage, spider_name)
      end)

      assert {:failed, :storage_worker_not_running} =
               Crawly.BatchCommitter.commit(
                 spider_name,
                 context.crawl_id,
                 "batch_1",
                 [req("/1")],
                 [%{id: 1}]
               )

      assert {:stored_items, 0} = Crawly.DataStorage.stats(spider_name)

      assert %{accepted: 0, duplicate: 0, failed: 1} =
               Crawly.BatchCommitter.stats(spider_name, context.crawl_id)
    end
  end

  describe "queue full" do
    test "a batch which does not fit into the queue is rejected as a whole",
         context do
      Application.put_env(:crawly, :requests_storage_max_size, 2)

      assert {:failed, :queue_full} =
               Crawly.BatchCommitter.commit(
                 context.spider_name,
                 context.crawl_id,
                 "batch_1",
                 [req("/1"), req("/2"), req("/3")],
                 [%{id: 1}]
               )

      # Neither requests nor items became visible
      assert {:stored_requests, 0} =
               Crawly.RequestsStorage.stats(context.spider_name)

      assert {:stored_items, 0} = Crawly.DataStorage.stats(context.spider_name)

      assert %{accepted: 0, duplicate: 0, failed: 1} =
               Crawly.BatchCommitter.stats(
                 context.spider_name,
                 context.crawl_id
               )

      # The same batch is accepted once there is enough capacity
      Application.put_env(:crawly, :requests_storage_max_size, 10)

      assert :accepted =
               Crawly.BatchCommitter.commit(
                 context.spider_name,
                 context.crawl_id,
                 "batch_1",
                 [req("/1"), req("/2"), req("/3")],
                 [%{id: 1}]
               )

      assert {:stored_requests, 3} =
               Crawly.RequestsStorage.stats(context.spider_name)

      assert {:stored_items, 1} = Crawly.DataStorage.stats(context.spider_name)
    end

    test "capacity accounts for already stored requests", context do
      Application.put_env(:crawly, :requests_storage_max_size, 3)

      assert :accepted =
               Crawly.BatchCommitter.commit(
                 context.spider_name,
                 context.crawl_id,
                 "batch_1",
                 [req("/1"), req("/2")],
                 []
               )

      assert {:failed, :queue_full} =
               Crawly.BatchCommitter.commit(
                 context.spider_name,
                 context.crawl_id,
                 "batch_2",
                 [req("/3"), req("/4")],
                 []
               )

      assert {:stored_requests, 2} =
               Crawly.RequestsStorage.stats(context.spider_name)
    end
  end

  describe "pipeline exceptions" do
    test "a crashing pipeline aborts the batch and leaves storages untouched",
         context do
      Application.put_env(:crawly, :pipelines, [CrashingPipeline])

      capture_log(fn ->
        assert {:failed, _reason} =
                 Crawly.BatchCommitter.commit(
                   context.spider_name,
                   context.crawl_id,
                   "batch_1",
                   [req("/1")],
                   [%{id: 1}, %{id: 2}]
                 )
      end)

      assert {:stored_items, 0} = Crawly.DataStorage.stats(context.spider_name)

      assert {:stored_requests, 0} =
               Crawly.RequestsStorage.stats(context.spider_name)

      assert %{accepted: 0, duplicate: 0, failed: 1} =
               Crawly.BatchCommitter.stats(
                 context.spider_name,
                 context.crawl_id
               )

      # Once the pipeline is fixed, the same batch goes through
      Application.put_env(:crawly, :pipelines, [])

      assert :accepted =
               Crawly.BatchCommitter.commit(
                 context.spider_name,
                 context.crawl_id,
                 "batch_1",
                 [req("/1")],
                 [%{id: 1}, %{id: 2}]
               )

      assert {:stored_items, 2} = Crawly.DataStorage.stats(context.spider_name)

      assert {:stored_requests, 1} =
               Crawly.RequestsStorage.stats(context.spider_name)
    end

    test "items dropped by pipelines are not counted as stored", context do
      Application.put_env(:crawly, :pipelines, [
        {Crawly.Pipelines.Validate, fields: [:title]}
      ])

      assert :accepted =
               Crawly.BatchCommitter.commit(
                 context.spider_name,
                 context.crawl_id,
                 "batch_1",
                 [],
                 [%{title: "kept"}, %{no_title: "dropped"}]
               )

      assert {:stored_items, 1} = Crawly.DataStorage.stats(context.spider_name)
    end
  end

  describe "worker integration" do
    setup do
      :meck.expect(Crawly.Utils, :send_after, fn _, _, _ -> :ignore end)

      :meck.expect(HTTPoison, :get, fn _, _, _ ->
        {
          :ok,
          %HTTPoison.Response{
            status_code: 200,
            body: "Some page",
            headers: [],
            request: %{}
          }
        }
      end)

      on_exit(fn -> :meck.unload() end)
      :ok
    end

    test "parsed batches are committed idempotently by the worker", context do
      spider_name = BatchTestSpider
      crawl_id = context.crawl_id

      {:ok, requests_storage_pid} =
        Crawly.RequestsStorage.start_worker(spider_name, crawl_id)

      {:ok, data_storage_pid} =
        Crawly.DataStorage.start_worker(spider_name, crawl_id)

      on_exit(fn ->
        :ok = TestUtils.stop_process(requests_storage_pid)
        :ok = TestUtils.stop_process(data_storage_pid)
        wait_for_deregistration(Crawly.RequestsStorage, spider_name)
        wait_for_deregistration(Crawly.DataStorage, spider_name)
      end)

      worker_state = %Crawly.Worker{
        spider_name: spider_name,
        crawl_id: crawl_id,
        backoff: 10_000
      }

      :ok =
        Crawly.RequestsStorage.store(
          spider_name,
          req("https://www.example.com")
        )

      Crawly.Worker.handle_info(:work, worker_state)

      assert {:stored_items, 1} = Crawly.DataStorage.stats(spider_name)
      assert {:stored_requests, 1} = Crawly.RequestsStorage.stats(spider_name)

      assert %{accepted: 1, duplicate: 0, failed: 0} =
               Crawly.BatchCommitter.stats(spider_name, crawl_id)

      # Re-deliver the same response for the same attempt: the batch must
      # not be committed again.
      :ok =
        Crawly.RequestsStorage.store(
          spider_name,
          req("https://www.example.com")
        )

      Crawly.Worker.handle_info(:work, worker_state)

      assert {:stored_items, 1} = Crawly.DataStorage.stats(spider_name)
      assert {:stored_requests, 1} = Crawly.RequestsStorage.stats(spider_name)

      assert %{accepted: 1, duplicate: 1, failed: 0} =
               Crawly.BatchCommitter.stats(spider_name, crawl_id)
    end
  end

  defp req(path) do
    Crawly.Request.new("https://www.example.com#{path}", [], [], [])
  end

  defp restore_env(key, nil), do: Application.delete_env(:crawly, key)
  defp restore_env(key, value), do: Application.put_env(:crawly, key, value)

  # Storage workers are de-registered asynchronously (via a DOWN message),
  # wait for it so later tests see a clean storage state.
  defp wait_for_deregistration(storage, spider_name, attempts \\ 100)
  defp wait_for_deregistration(_storage, _spider_name, 0), do: :ok

  defp wait_for_deregistration(storage, spider_name, attempts) do
    case storage.stats(spider_name) do
      {:error, _} ->
        :ok

      _ ->
        Process.sleep(10)
        wait_for_deregistration(storage, spider_name, attempts - 1)
    end
  end
end
