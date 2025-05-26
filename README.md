# Project Overview

`evoland-plus` is a software project that aims to integrate statistical land use/land cover change (LULCC).
It is based on the [LULCC-CH-private](https://github.com/blenback/LULCC-CH-private) project, but to achieve expanding analytical goals, it is being remodelled into an R package.
Part of the workflow (that is: the LULCC extrapolation) relies on a coupling of R to [Dinamica EGO](https://dinamicaego.com/).

## How do I get set up?

To ease development and deployment, the many software dependencies are managed in a [devcontainer](.devcontainer/devcontainer.json), which in turn is built by [rocker-geospatial-dinamica](https://github.com/ethzplus/rocker-geospatial-dinamica).
This is particularly well integrated with VS Code, [see the docs](https://code.visualstudio.com/docs/devcontainers/containers).
If you prefer to work in a native `renv` environment, you're welcome to use `renv::init()`.
You will be given a nice prompt where to read the project dependencies from.
Just go with the [`DESCRIPTION`](./DESCRIPTION), since you would otherwise end up installing loads of seldom-used packages - you can always instal those later.

> [!CAUTION]
> This repository is undergoing heavy modifications; don't expect feature or interface stability.
> If you write any logic that depends on the package's current state, you should clearly reference the exact commit you base your work on.

## To Do

TODOs are being tracked in [this GitHub project](https://github.com/orgs/ethzplus/projects/1) and the [issues](issues) on this repository.
TODO and FIXME comments are used to indicate problematic places throughout the codebase.
Generally, the codebase will be factored into smaller pieces until individual pieces of logic become unit-testable.

## History of the repo

The commit history is rewritten (using [git-filter-repo](https://github.com/newren/git-filter-repo)) to omit large binary files above 1MiB and made all paths lowercase to avoid trouble with case insensitive file systems.
Yet, there may be historic commits in there that we don't want to keep - hence the "with-baggage" moniker.

A bit of guesswork has gone into the reconstruction of file names and paths: occasionally, you may find a file that was referenced in some place but committed to the repository under a different name.
Where I was reasonably confident that two paths should match up, I changed either the reference or renamed the referred object.
