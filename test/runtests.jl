using Sialia
using Test

@testset "Sialia.jl" begin
    include("eventdispatcher_tests.jl")
    include("eventstore_tests.jl")
end
