mutable struct EventMetadata
    datetime::Dates.DateTime
    correlation_id::UUID
    causation_id::UUID
    published::Bool
    position::Union{Int,Nothing}
end

struct EventContext
    name::String
end

struct EventAggregate
    name::String
    id::Union{UUID,Nothing}
    revision::Union{Int,Nothing}
end

struct StreamInfo
    type::Symbol
    context_name::String
    aggregate_name::String
    event_name::String
end

function Base.print(io::IO, info::StreamInfo)
    print(io, "$(info.type).$(info.context_name).$(info.aggregate_name).$(info.event_name)")
end

function Base.parse(::Type{StreamInfo}, str::AbstractString)
    matched = match(r"^([^.]+)\.([^.]+)\.([^.]+)\.([^.]+)$", str)
    if isnothing(matched)
        throw(ArgumentError("invalid stream identifier $(repr(str))"))
    end

    StreamInfo(Symbol(matched[1]), matched[2], matched[3], matched[4])
end

const EventData = Dict
const EventCustom = Dict
const EventState = Dict

const EventTypes = Set([
    :command,
    :domain,
    :error
])

struct Event
    id::UUID
    publish_id::Union{String, Nothing}
    name::String
    type::Symbol
    context::EventContext
    aggregate::EventAggregate
    metadata::EventMetadata
    custom::EventCustom
    data::EventData
    state::Union{EventState,Nothing}
end

function Event(
    context_name::String,
    aggregate_name::String,
    event_name::String;
    type::Symbol=:domain,
    id::UUID=UUIDs.uuid4(),
    publish_id::Union{String,Nothing}=nothing,
    aggregate_id::Union{UUID,Nothing}=nothing,
    datetime::Dates.DateTime=Dates.now(Dates.UTC),
    previous::Union{Event,Nothing}=nothing,
    correlation_id::Union{UUID,Nothing}=nothing,
    causation_id::Union{UUID,Nothing}=nothing,
    published::Bool=false,
    custom::EventCustom=EventCustom(),
    data::EventData=EventData(),
    position::Union{Int,Nothing}=nothing,
    revision::Union{Int,Nothing}=nothing,
    state::Union{EventState,Nothing}=nothing
)
    # Validate the event
    if isempty(context_name) || !isnothing(match(r"\.", context_name))
        throw(ArgumentError("invalid context name $(repr(context_name))"))
    end

    if isempty(aggregate_name) || !isnothing(match(r"\.", aggregate_name))
        throw(ArgumentError("invalid aggregate name $(repr(aggregate_name))"))
    end

    if isempty(event_name) || !isnothing(match(r"\.", event_name))
        throw(ArgumentError("invalid event name $(repr(event_name))"))
    end

    if !(type in EventTypes)
        throw(ArgumentError("invalid event type $(repr(type))"))
    end

    if isnothing(correlation_id)
        if isnothing(previous)
            throw(ArgumentError("either a previous event or a correlation id must be provided"))
        end

        correlation_id = previous.metadata.correlation_id
    end

    if isnothing(causation_id)
        if isnothing(previous)
            throw(ArgumentError("either a previous event or a causation id must be provided"))
        end

        causation_id = previous.id
    end

    return Event(
        id,
        publish_id,
        event_name,
        type,
        EventContext(context_name),
        EventAggregate(aggregate_name, aggregate_id, revision),
        EventMetadata(datetime, correlation_id, causation_id, published, position),
        custom,
        data,
        state,
    )
end

function stream_info(event::Event) :: StreamInfo
    StreamInfo(event.type, event.context.name, event.aggregate.name, event.name)
end

function Base.parse(::Type{Event}, doc::Mongoc.BSON) :: Event
    Event(
        doc["id"],
        nothing,
        doc["name"],
        :domain,
        EventContext(
            doc["context"]["name"]
        ),
        EventAggregate(
            doc["aggregate"]["name"],
            doc["aggregate"]["id"],
            doc["aggregate"]["revision"],
        ),
        EventMetadata(
            doc["metadata"]["datetime"],
            doc["metadata"]["correlation_id"],
            doc["metadata"]["causation_id"],
            doc["metadata"]["published"],
            doc["metadata"]["position"],
        ),
        Dict(doc["custom"]),
        Dict(doc["data"]),
        nothing,
    )
end
