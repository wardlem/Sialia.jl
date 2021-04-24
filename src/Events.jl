module Events

using Base: UUID
import UUIDs
import Dates
import Random
import Redis
import CBOR

export Event, EventDispatcher, EventException, StreamInfo, get_dispatcher

struct EventException <: Exception
    msg::AbstractString
end

include("./events/Event.jl")
include("./events/EventDispatcher.jl")

function get_dispatcher(group_id, consumer_id; claim_time=Dates.Minute(15))
    host = get(ENV, "REDIS_DISPATCHER_HOST", "127.0.0.1")
    port = parse(UInt16, get(ENV, "REDIS_DISPATCHER_PORT", "6379"))
    password = get(ENV, "REDIS_DISPATCHER_PASSWORD", "")
    db = parse(UInt, get(ENV, "REDIS_DISPATCHER_DB", "0"))
    RedisEventDispatcher(group_id, consumer_id; host, port, password, db, claim_time)
end

end
