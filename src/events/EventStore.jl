abstract type EventStore end

const DEFAULT_MONGO_URI = "mongodb://127.0.0.1:27021,127.0.0.1:27022,127.0.0.1:27023/?replicaSet=rst"
const AUTO_SNAPSHOT = 100

mutable struct MongoEventStore <: EventStore
    client::Union{Mongoc.Client,Nothing}
    uri::String
    dbname::String
    namespace::Union{String,Nothing}
    state::ServiceState
    auto_snapshot::UInt
end

function MongoEventStore(;uri::String=DEFAULT_MONGO_URI, dbname::String="eventstore", namespace::Union{String,Nothing}=nothing, auto_snapshot=AUTO_SNAPSHOT)
    MongoEventStore(nothing, uri, dbname, namespace, stopped, auto_snapshot)
end

function collection_name(store::MongoEventStore, type::String)
    if isnothing(store.namespace)
        type
    else
        `$(store.namespace)_$(type)`
    end
end

events_collection_name(store::MongoEventStore) = collection_name(store, "events")
snapshots_collection_name(store::MongoEventStore) = collection_name(store, "snapshots")
counters_collection_name(store::MongoEventStore) = collection_name(store, "counters")

function run!(store::MongoEventStore)
    if store.state == running
        throw(EventException("The event store is already running"))
    end

    store.state = running

    store.client = Mongoc.Client(store.uri)

    # Initialize required collections
    client = store.client
    database = client[store.dbname]

    events_name = events_collection_name(store)
    snapshots_name = snapshots_collection_name(store)
    counters_name = counters_collection_name(store)

    create_events_indexes_cmd = BSON(
        "createIndexes" => events_name,
        "indexes" => [
            BSON(
                "key" => BSON("id" => 1), 
                "name" => "$(events_name)_id",
                "unique" => true,
            ),
            BSON(
                "key" => BSON("aggregate.id" => 1), 
                "name" => "$(events_name)_aggregate_id"
            ),
            BSON(
                "key" => BSON("aggregate.id" => 1, "aggregate.revision" => 1), 
                "name" => "$(events_name)_aggregate_id_revision",
                "unique" => true,
            ),
            BSON(
                "key" => BSON("metadata.position" => 1), 
                "name" => "$(events_name)_position"
            ),
        ]
    )

    create_snapshots_indexes_cmd = BSON(
        "createIndexes" => snapshots_name,
        "indexes" => [ 
            BSON(
                "key" => BSON("aggregate.id" => 1),
                "name" => "$(snapshots_name)_aggregate_id",
                "unique" => true,
            ),
        ]
    )

    create_counters_indexes_cmd = BSON(
        "createIndexes" => counters_name,
        "indexes" => [ 
            BSON(
                "key" => BSON("counter_id" => 1),
                "name" => "$(counters_name)_counter_id",
                "unique" => true,
            ),
        ]
    )

    reply = Mongoc.write_command(database, create_events_indexes_cmd)
    @assert reply["ok"] == 1
    reply = Mongoc.write_command(database, create_snapshots_indexes_cmd)
    @assert reply["ok"] == 1
    reply = Mongoc.write_command(database, create_counters_indexes_cmd)
    @assert reply["ok"] == 1

    # Initialize the counter
    @assert !isnothing(create_counter(store, "events"))
    nothing
end

# Drops the collections...intended to be used for testing only
function reset!(store::MongoEventStore)
    client = Mongoc.Client(store.uri)
    database = client[store.dbname]

    events_name = events_collection_name(store)
    snapshots_name = snapshots_collection_name(store)
    counters_name = counters_collection_name(store)

    drop_events_cmd = BSON(
        "drop" => events_name,
        "comment" => "reset event store",
    )

    drop_snapshots_cmd = BSON(
        "drop" => snapshots_name,
        "comment" => "reset event store",
    )

    drop_counters_cmd = BSON(
        "drop" => counters_name,
        "comment" => "reset event store",
    )

    try
        Mongoc.write_command(database, drop_events_cmd)
        Mongoc.write_command(database, drop_snapshots_cmd)
        Mongoc.write_command(database, drop_counters_cmd)
        nothing
    catch _err
        # Ignore
    end

    finalize(client)
    nothing
end

function stop!(store::MongoEventStore)
    if store.state == stopped
        throw(EventException("The event store is not running"))
    end

    finalize(store.client)
    store.client = nothing

    store.state = stopped
end

function create_counter(store::MongoEventStore, counter_id::String)
    client = store.client
    database = client[store.dbname]

    query = BSON(
        "counter_id" => counter_id
    )

    update = BSON(
        "\$setOnInsert" => BSON(
            "seq" => 0,
            "counter_id" => counter_id,
        ),
    )

    command = BSON(
        "findAndModify" => counters_collection_name(store),
        "query" => query,
        "update" => update,
        "new" => true,
        "upsert" => true,
    )

    reply = Mongoc.write_command(
        database,
        command,
    )

    return reply["value"]
end

function next_sequence(store::MongoEventStore, database::Mongoc.DatabaseSession, counter_id::String)
    # This should be done transactionally which is why
    # a database instance is required in addition to
    # the store instance

    query = BSON(
        "counter_id" => counter_id
    )

    update = BSON(
        "\$inc" => BSON("seq" => 1)
    )

    command = BSON(
        "findAndModify" => counters_collection_name(store),
        "query" => query,
        "update" => update,
        "new" => true,
    )

    options = BSON()
    err_ref = Ref{Mongoc.BSONError}()
    ok = Mongoc.mongoc_client_session_append(
        Mongoc.get_session(database).handle,
        options.handle,
        err_ref,
    )

    if !ok
        throw(err_ref[])
    end

    reply = Mongoc.write_command(
        database.database,
        command;
        options,
    )

    doc = reply["value"]
    if isnothing(doc)
        throw(EventException("failed to get next sequence for $counter_id"))
    end

    doc["seq"]
end

function last_sequence(store::MongoEventStore, counter_id::String)
    client = store.client
    database = client[store.dbname]
    collection = database[counters_collection_name(store)]

    query = BSON(
        "counter_id" => counter_id
    )

    doc = Mongoc.find_one(
        collection,
        query,
    )

    if isnothing(doc)
        throw(EventException("failed to get last sequence for $counter_id"))
    end

    doc["seq"]
end

function aggregate_events_iterator(store::MongoEventStore, aggregate_id::UUID; from_revision=1, to_revision=2 ^ 31 - 1)
    client = store.client
    database = client[store.dbname]
    collection = database[events_collection_name(store)]

    query = BSON(
        "aggregate.id" => aggregate_id,
        "aggregate.revision" => BSON(
            "\$gte" => from_revision,
            "\$lte" => to_revision,
        ),
    )

    options = BSON(
        "sort" => BSON("aggregate.revision" => 1),
        "projection" => BSON("_id" => 0),
    )

    cursor = Mongoc.find(collection, query; options)

    EventsIterator(cursor)
end

function unpublished_events_iterator(store::MongoEventStore)
    client = store.client
    database = client[store.dbname]
    collection = database[events_collection_name(store)]

    query = BSON(
        "metadata.published" => false,
    )

    options = BSON(
        "sort" => BSON("metadata.position" => 1),
        "projection" => BSON("_id" => 0),
    )

    cursor = Mongoc.find(collection, query; options)

    EventsIterator(cursor)
end

function save_events(store::MongoEventStore, new_events::Vector{Event})
    committed_events = Vector{Event}()
    client = store.client

    # Run transactionally
    Mongoc.transaction(client) do session
        database = session[store.dbname]
        collection = database[events_collection_name(store)]

        for event in new_events
            if event.aggregate.revision < 1
                throw(ArgumentError("aggregate revision must be greater than 0"))
            end

            position = next_sequence(store, database, "events")
            event.metadata.position = position

            doc = BSON(
                "id" => event.id,
                "name" => event.name,
                "context" => BSON(
                    "name" => event.context.name,
                ),
                "aggregate" => BSON(
                    "name" => event.aggregate.name,
                    "id" => event.aggregate.id,
                    "revision" => event.aggregate.revision,
                ),
                "metadata" => BSON(
                    "datetime" => event.metadata.datetime,
                    "correlation_id" => event.metadata.correlation_id,
                    "causation_id" => event.metadata.causation_id,
                    "published" => event.metadata.published,
                    "position" => event.metadata.position,
                ),
                "custom" => event.custom,
                "data" => event.data,
            )

            try
                push!(collection, doc)
            catch err
                # TODO: Make sure this is an error we expect
                # and map the error appropriately
                io = IOBuffer()
                show(io, err)
                errtext = String(take!(io))
                if err.code == 11000 && !isnothing(match(r"aggregate_id_revision", errtext))
                    throw(EventException("duplicate revision"))
                end

                throw(err)
            end

            # Auto-snapshot
            if event.aggregate.revision % store.auto_snapshot == 0 && !isnothing(event.state)
                save_snapshot(store, database, event)
            end

            push!(committed_events, event)
        end
    end

    committed_events
end

function aggregate_last_event(store::MongoEventStore, aggregate_id::UUID)
    client = store.client
    database = client[store.dbname]
    collection = database[events_collection_name(store)]

    query = BSON(
        "aggregate.id" => aggregate_id,
    )

    options = BSON(
        "sort" => BSON("aggregate.revision" => -1),
        "projection" => BSON("_id" => 0),
    )

    doc = Mongoc.find_one(collection, query; options)

    if isnothing(doc)
        doc
    else
        parse(Event, doc)
    end
end

function mark_published_events(store::MongoEventStore, events::Vector{Event})
    client = store.client
    database = client[store.dbname]
    collection = database[events_collection_name(store)]

    query = BSON(
        "id" => BSON(
            "\$in" => map(events) do event event.id end,
        ),
    )

    update = BSON(
        "\$set" => BSON("metadata.published" => true),
    )

    result = Mongoc.update_many(collection, query, update)
    for event in events
        event.metadata.published = true
    end

    events
end

function get_snapshot(store::MongoEventStore, aggregate_id::UUID)
    client = store.client
    database = client[store.dbname]
    collection = database[snapshots_collection_name(store)]

    query = BSON(
        "aggregate.id" => aggregate_id,
    )

    options = BSON(
        "projection" => BSON("_id" => 0),
    )

    doc = Mongoc.find_one(collection, query; options)

    # TODO: Automatically convert to an aggregate
    doc
end

function save_snapshot(store::MongoEventStore, database::Mongoc.DatabaseSession, event::Event)
    if isnothing(event.state)
        throw(ArgumentError("event must have a state to create a snapshot"))
    end

    collection = database[snapshots_collection_name(store)]

    query = BSON(
        "aggregate.id" => event.aggregate.id,
    )

    setter = BSON(
        "\$set" => BSON(
            "aggregate" => BSON(
                "name" => event.aggregate.name,
                "id" => event.aggregate.id,
                "revision" => event.aggregate.revision,
            ),
            "state" => BSON(event.state),   
        )
    )

    options = BSON(
        "upsert" => true,
    )

    Mongoc.update_one(collection, query, setter; options)

    nothing
end

function replay_events(store::MongoEventStore; from=1, to=2 ^ 31 -1)
    client = store.client
    database = client[store.dbname]
    collection = database[events_collection_name(store)]

    query = BSON(
        "metadata.position" => BSON(
            "\$gte" => from,
            "\$lte" => to,
        ),
    )

    options = BSON(
        "sort" => BSON("metadata.position" => 1),
        "projection" => BSON("_id" => 0),
    )

    cursor = Mongoc.find(collection, query; options)

    EventsIterator(cursor)
end

struct EventsIterator
    cursor::Mongoc.Cursor
end

function Base.iterate(iter::EventsIterator, state::Nothing=nothing)
    next = Base.iterate(iter.cursor)
    if isnothing(next)
        nothing
    else
        (parse(Event, next[1]), nothing)
    end
end

function Base.collect(iter::EventsIterator)
    out = Vector{Event}()

    for event in iter
        push!(out, event)
    end

    out
end
