@testset "Event store" begin
    store = Sialia.Events.get_event_store(;auto_snapshot=2)

    # Clean slate for testing
    Sialia.Events.reset!(store)

    @testset "Start" begin
        # Create connection, indexes, etc.
        Sialia.Events.run!(store)
        @test !isnothing(store.client)

        # Already started
        @test_throws Sialia.Events.EventException Sialia.Events.run!(store)
    end

    id1 = UUIDs.uuid4()
    id2 = UUIDs.uuid4()

    @testset "Saves events / snapshots" begin
        # Store an event
        event1 = Sialia.Events.Event(
            "testcontext",
            "testaggregate",
            "testevent";
            aggregate_id=id1,
            correlation_id=UUIDs.uuid4(),
            causation_id=UUIDs.uuid4(),
            data=Dict("testkey" => "testdata"),
            revision=Int(1),
            state=Dict("id" => id1, "revision" => Int(1)),
        )

        @test Sialia.Events.last_sequence(store, "events") == 0
        stored = Sialia.Events.save_events(store, [event1])
        @test Sialia.Events.last_sequence(store, "events") == 1
        @test length(stored) == 1
        @test stored[1].metadata.position == 1
        @test isnothing(Sialia.Events.get_snapshot(store, id1))

        event1dup = Sialia.Events.Event(
            "testcontext",
            "testaggregate",
            "testevent";
            aggregate_id=id1,
            correlation_id=UUIDs.uuid4(),
            causation_id=UUIDs.uuid4(),
            data=Dict("testkey" => "testdata"),
            revision=Int(1),
            state=Dict("id" => id1, "revision" => Int(1)),
        )

        @test_throws Sialia.Events.EventException Sialia.Events.save_events(store, [event1dup])

        # Sequence does not update when save errors
        @test Sialia.Events.last_sequence(store, "events") == 1

        # Store another event
        event2 = Sialia.Events.Event(
            "testcontext",
            "testaggregate",
            "testevent";
            aggregate_id=id1,
            correlation_id=UUIDs.uuid4(),
            causation_id=UUIDs.uuid4(),
            data=Dict("testkey" => "testdata2"),
            revision=Int(2),
            state=Dict("id" => id1, "revision" => Int(2)),
        )

        stored = Sialia.Events.save_events(store, [event2])
        @test Sialia.Events.last_sequence(store, "events") == 2
        @test length(stored) == 1
        @test stored[1].metadata.position == 2

        # Should create a snapshot
        @test Sialia.Events.get_snapshot(store, id1)["state"] == event2.state

        # Make some more events
        events = Vector{Sialia.Events.Event}()
        for i in 3:6
            # Store another event
            event = Sialia.Events.Event(
                "testcontext",
                "testaggregate",
                "testevent";
                aggregate_id=id1,
                correlation_id=UUIDs.uuid4(),
                causation_id=UUIDs.uuid4(),
                data=Dict("testkey" => "testdata$i"),
                revision=Int(i),
                state=Dict("id" => id1, "revision" => Int(i)),
            )

            push!(events, event)
        end

        stored = Sialia.Events.save_events(store, events)

        @test length(stored) == 4
        @test stored[4].metadata.position == 6

        @test Sialia.Events.get_snapshot(store, id1)["state"]["revision"] == 6

        # Store an event for a different aggregate
        event3 = Sialia.Events.Event(
            "testcontext",
            "testaggregate",
            "testevent";
            aggregate_id=id2,
            correlation_id=UUIDs.uuid4(),
            causation_id=UUIDs.uuid4(),
            data=Dict("testkey" => "testdata"),
            revision=Int(1),
            state=Dict("id" => id2, "revision" => Int(1)),
        )

        stored = Sialia.Events.save_events(store, [event3])
        @test length(stored) == 1
        @test stored[1].metadata.position == 7
        @test isnothing(Sialia.Events.get_snapshot(store, id2))
    end

    @testset "Aggregate events" begin
        iter = Sialia.Events.aggregate_events_iterator(store, id1)
        last_revision = 0
        for event in iter
            @test event.aggregate.revision - 1 == last_revision
            @test event.aggregate.id == id1
            last_revision = event.aggregate.revision
        end

        @test last_revision == 6

        iter = Sialia.Events.aggregate_events_iterator(store, id1, from_revision=2, to_revision=5)
        last_revision = 1
        for event in iter
            @test event.aggregate.revision - 1 == last_revision
            @test event.aggregate.id == id1
            last_revision = event.aggregate.revision
        end

        @test last_revision == 5
    end

    @testset "Unpublished events / Mark as published" begin
        iter = Sialia.Events.unpublished_events_iterator(store)
        unpublished = Base.collect(iter)

        @test length(unpublished) == 7

        Sialia.Events.mark_published_events(store, unpublished)

        iter = Sialia.Events.unpublished_events_iterator(store)
        unpublished = Base.collect(iter)

        @test length(unpublished) == 0
    end

    @testset "Last event" begin
        event = Sialia.Events.aggregate_last_event(store, id1)
        @test event.aggregate.revision == 6
        @test event.metadata.position == 6

        event = Sialia.Events.aggregate_last_event(store, id2)
        @test event.aggregate.revision == 1
        @test event.metadata.position == 7

        event = Sialia.Events.aggregate_last_event(store, UUIDs.uuid4())
        @test isnothing(event)
    end

    @testset "Replay" begin
        iter = Sialia.Events.replay_events(store)
        last_position = 0
        for event in iter
            @test event.metadata.position - 1 == last_position
            last_position = event.metadata.position
        end

        @test last_position == 7

        iter = Sialia.Events.replay_events(store, from=2, to=5)
        last_position = 1
        for event in iter
            @test event.metadata.position - 1 == last_position
            last_position = event.metadata.position
        end

        @test last_position == 5
    end

    @testset "Stop" begin
        Sialia.Events.stop!(store)
        @test isnothing(store.client)

        # Already stopped
        @test_throws Sialia.Events.EventException Sialia.Events.stop!(store)
    end

    @testset "Restart" begin
        # Can restart from existing data
        Sialia.Events.run!(store)
        Sialia.Events.stop!(store)
    end
end
