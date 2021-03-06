@enum OutboxKind send=1 acknowledge

abstract type EventDispatcher end

mutable struct RedisEventDispatcher <: EventDispatcher
    conn::Redis.RedisConnection
    group_id::AbstractString
    consumer_id::AbstractString
    state::ServiceState
    inbox::Channel{Event}
    outbox::Channel{Tuple{OutboxKind, Event}}
    claim_time::Dates.Period
end

function RedisEventDispatcher(group_id::AbstractString, consumer_id::AbstractString; host="127.0.0.1", port=6379, password="", db=0, outbox_size=32, inbox_size=32, claim_time=Dates.Minute(15))
    conn = Redis.RedisConnection(;host, port, password, db)
    inbox = Channel{Event}(inbox_size)
    outbox = Channel{Tuple{OutboxKind, Event}}(outbox_size)
    RedisEventDispatcher(conn, group_id, consumer_id, stopped, inbox, outbox, claim_time)
end

function is_connected(dispatcher::RedisEventDispatcher)
    Redis.is_connected(dispatcher.conn)
end

function is_running(dispatcher::RedisEventDispatcher)
    dispatcher.state == running
end

function _redis_data_to_event(stream, eventarr)
    publish_id = eventarr[1]
    eventdata = eventarr[2]
    recorddata = Dict{String,Any}()
    for ind = 1:2:size(eventdata, 1)
        recorddata[eventdata[ind]] = eventdata[ind + 1]
    end

    id = UUID(recorddata["id"])
    correlation_id = UUID(recorddata["meta.corr_id"])
    causation_id = UUID(recorddata["meta.caus_id"])
    datetime = Dates.DateTime(recorddata["meta.datetime"])
    data = CBOR.decode(Vector{UInt8}(get(recorddata, "data", "\xa0")))
    custom = CBOR.decode(Vector{UInt8}(get(recorddata, "custom", "\xa0")))
    aggregate_id_str = get(recorddata, "aggregate.id", nothing)
    aggregate_id = if isnothing(aggregate_id_str) 
        nothing
    else 
        UUID(aggregate_id_str)
    end
    stream_info = parse(StreamInfo, stream)

    Event(
        stream_info.context_name,
        stream_info.aggregate_name,
        stream_info.event_name,
        type=stream_info.type,
        id=id,
        publish_id=publish_id,
        aggregate_id=aggregate_id,
        datetime=datetime,
        correlation_id=correlation_id,
        causation_id=causation_id,
        published=true,
        custom=custom,
        data=data,
    )
end

function _run(dispatcher::RedisEventDispatcher, streams::Vector{String})
    # Create a group for each stream
    for stream in streams
        try
            redis_args = ["XGROUP", "CREATE", stream, dispatcher.group_id, "\$", "MKSTREAM"]
            Redis.execute_command(dispatcher.conn, redis_args)
        catch _err
            # TODO: Check the error type is correct
        end
    end

    last_claim = Dates.Date(0)

    # Endless loop to poll for items
    while dispatcher.state == running
        while isready(dispatcher.outbox)
            try
                (kind, event) = take!(dispatcher.outbox)
                redis_args = if kind == acknowledge
                    ["XACK", string(stream_info(event)), dispatcher.group_id, event.publish_id]
                elseif kind == send
                    eventid = if isnothing(event.publish_id) "*" else event.publish_id end
                    redis_args = [
                        "XADD",
                        string(stream_info(event)),
                        eventid,
                        "id", string(event.id),
                        "meta.corr_id", string(event.metadata.correlation_id),
                        "meta.caus_id", string(event.metadata.causation_id),
                        "meta.datetime", string(event.metadata.datetime),
                        "data", CBOR.encode(event.data),
                        "custom", CBOR.encode(event.custom),
                    ]

                    if !isnothing(event.aggregate.id)
                        redis_args =  [redis_args; ["aggregate.id", event.aggregate.id]]
                    end

                    redis_args
                else
                    nothing
                end

                if !isnothing(redis_args)
                    Redis.execute_command(dispatcher.conn, redis_args)
                end

                # Prevent blocking on the channel put!
                # If the inbox is full, the process is probably overwhelmed
                max_count = dispatcher.inbox.sz_max - length(dispatcher.inbox.data)

                # If there's no space we don't need to fetch more items
                if max_count != 0
                    redis_args = ["XREADGROUP", "GROUP", dispatcher.group_id, dispatcher.consumer_id, "COUNT", max_count, "STREAMS", streams..., ">"]
                    entries = Redis.execute_command(dispatcher.conn, redis_args)

                    if !isnothing(entries)
                        for streamrecord in entries
                            stream = streamrecord[1]
                            events = streamrecord[2]
                            for eventdata in events
                                try
                                    event = _redis_data_to_event(stream, eventdata)
                                    put!(dispatcher.inbox, event)
                                catch err
                                    println("Receive error: ", err)
                                end
                            end
                        end
                    end
                end

                if last_claim < (Date.now() + dispatcher.claim_time)
                    # Claim old events
                    last_claim = Date.now()
                    for stream in Random.shuffle(streams)
                        max_count = dispatcher.inbox.sz_max - length(dispatcher.inbox.data)
                        if max_count == 0
                            break
                        end

                        redis_args = [
                            "XAUTOCLAIM",
                            stream,
                            dispatcher.group_id,
                            dispatcher.consumer_id,
                            Dates.Millisecond(dispatcher.claim_time).value,
                            "0-0",
                            "COUNT",
                            max_count,
                        ]

                        result = Redis.execute_command(dispatcher.conn, redis_args)

                        if !isnothing(result) && length(result) > 1
                            for eventdata in result[2]
                                try
                                    event = _redis_data_to_event(stream, eventdata)
                                    put!(dispatcher.inbox, event)
                                catch err
                                    println("Receive error: ", err)
                                end
                            end
                        end
                    end
                end
            catch err
                println("$(kind) error: ", err)
            end
        end

        if length(dispatcher.inbox.data) == 0
            sleep(0.1)
        end
    end
end

function run!(dispatcher::RedisEventDispatcher, streams::Vector{StreamInfo})
    if dispatcher.state == running
        throw(EventException("The dispatcher is already running"))
    end

    # Validate the streams

    streams = map(streams) do stream
        try
            if !(stream.type in EventTypes)
                throw(ArgumentError("stream has invalid type $(repr(stream))"))
            end

            string(stream)
        catch _err
            throw(ArgumentError("invalid stream $(repr(stream))"))
        end
    end
    dispatcher.state = running

    @async _run(dispatcher, streams)
end

function stop!(dispatcher::RedisEventDispatcher)
    if dispatcher.state == stopped
        throw(EventException("The dispatcher is not running"))
    end

    dispatcher.state = stopped
end

function ack!(dispatcher::RedisEventDispatcher, event::Event)
    put!(dispatcher.outbox, (acknowledge, event))
end

function Base.put!(dispatcher::RedisEventDispatcher, event::Event)
    put!(dispatcher.outbox, (send, event))
end

function Base.take!(dispatcher::RedisEventDispatcher)
    take!(dispatcher.inbox)
end

function Base.isready(dispatcher::RedisEventDispatcher)
    isready(dispatcher.inbox)
end
