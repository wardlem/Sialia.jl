@testset "Event dispatcher" begin
    import UUIDs

    dispatcher = Sialia.Events.get_dispatcher("testgroup", "testconsumer")
    @test Sialia.Events.is_connected(dispatcher)
    @test Sialia.Events.is_running(dispatcher) == false

    streams = [Sialia.Events.StreamInfo(
        :domain,
        "testcontext",
        "testaggregate",
        "testevent"
    )]

    # Can't stop an already stopped dispatcher
    @test_throws Sialia.Events.EventException Sialia.Events.stop!(dispatcher)

    Sialia.Events.run!(dispatcher, streams)
    @test Sialia.Events.is_running(dispatcher) == true

    # Can't start an already started dispatcher
    @test_throws Sialia.Events.EventException Sialia.Events.run!(dispatcher, streams)

    testvalue = rand()
    correlation_id = UUIDs.uuid4()
    causation_id = UUIDs.uuid4()

    testevent = Sialia.Events.Event(
        "testcontext",
        "testaggregate",
        "testevent",
        correlation_id=correlation_id,
        causation_id=causation_id,
        data=Dict("testkey" => testvalue),
        custom=Dict("customkey" => "customvalue")
    )

    @test testevent.name == "testevent"
    @test testevent.context.name == "testcontext"
    @test testevent.aggregate.name == "testaggregate"
    @test testevent.aggregate.id == nothing
    @test testevent.metadata.correlation_id == correlation_id
    @test testevent.metadata.causation_id == causation_id
    @test testevent.metadata.published == false
    @test testevent.data["testkey"] == testvalue
    @test testevent.custom["customkey"] == "customvalue"

    put!(dispatcher, testevent)

    receivedevent = take!(dispatcher)

    @test typeof(receivedevent) == Sialia.Events.Event
    @test receivedevent.id == testevent.id
    @test receivedevent.name == "testevent"
    @test receivedevent.context.name == "testcontext"
    @test receivedevent.aggregate.name == "testaggregate"
    @test receivedevent.aggregate.id == nothing
    @test receivedevent.metadata.correlation_id == correlation_id
    @test receivedevent.metadata.causation_id == causation_id
    @test receivedevent.metadata.published == true
    @test receivedevent.data["testkey"] == testvalue
    @test receivedevent.custom["customkey"] == "customvalue"

    Sialia.Events.ack!(dispatcher, receivedevent)

    Sialia.Events.stop!(dispatcher)
    @test Sialia.Events.is_running(dispatcher) == false
end
