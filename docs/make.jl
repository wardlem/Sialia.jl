using Sialia
using Documenter

DocMeta.setdocmeta!(Sialia, :DocTestSetup, :(using Sialia); recursive=true)

makedocs(;
    modules=[Sialia],
    authors="Mark Wardle <mark@potient.com> and contributors",
    repo="https://github.com/wardlem/Sialia.jl/blob/{commit}{path}#{line}",
    sitename="Sialia.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://wardlem.github.io/Sialia.jl",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/wardlem/Sialia.jl",
)
