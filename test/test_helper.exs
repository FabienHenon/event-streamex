{:ok, _pid} = TestRepo.start_link([])
{:ok, _pid} = Utils.PubSub.start_link([])
{:ok, _pid} = TestDb.start_link(Application.get_all_env(:event_streamex))
Logger.put_application_level(:event_streamex, :warning)

ExUnit.start()
