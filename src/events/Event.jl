struct EventMetadata
    datetime::Dates.DateTime
    correlation_id::UUID
    causation_id::UUID
    published::Bool
end

struct EventContext
    name::String
end

struct EventAggregate
    name::String
    id::Union{UUID,Nothing}
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

const EventTypes = Set([
    :command,
    :domain,
    :error
])

struct Event
    id::UUID
    internal_id::Union{String, Nothing}
    name::String
    type::Symbol
    context::EventContext
    aggregate::EventAggregate
    metadata::EventMetadata
    custom::EventCustom
    # raw_data::Union{Vector{UInt8},Nothing}
    data::EventData
end

function Event(
    context_name::String,
    aggregate_name::String,
    event_name::String;
    type::Symbol=:domain,
    id::UUID=UUIDs.uuid4(),
    internal_id::Union{String,Nothing}=nothing,
    aggregate_id::Union{UUID,Nothing}=nothing,
    datetime::Dates.DateTime=Dates.now(Dates.UTC),
    previous::Union{Event,Nothing}=nothing,
    correlation_id::Union{UUID,Nothing}=nothing,
    causation_id::Union{UUID,Nothing}=nothing,
    published::Bool=false,
    custom::EventCustom=EventCustom(),
    data::EventData=EventData(),
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
        internal_id,
        event_name,
        type,
        EventContext(context_name),
        EventAggregate(aggregate_name, aggregate_id),
        EventMetadata(datetime, correlation_id, causation_id, published),
        custom,
        data,
    )
end

function stream_info(event::Event)
    StreamInfo(event.type, event.context.name, event.aggregate.name, event.name)
end
