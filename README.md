# Project Overview

> [!IMPORTANT]
> This repository has reached a point where its feature set and interface should remain stable.
> @mmyrte will continue his efforts at [evoland-plus](https://github.com/ethzplus/evoland-plus/).
> See the [wiki for an overview](https://github.com/ethzplus/evoland-plus/wiki) of that project.

`evoland-plus` is a software project that uses statistics to model land use/land cover change (LULCC).
It is based on the [LULCC-CH-private](https://github.com/blenback/LULCC-CH-private) project, but to achieve expanding analytical goals, it is being remodelled into an R package.
Part of the workflow (that is: the LULCC allocation) relies on a coupling of R to [Dinamica EGO](https://dinamicaego.com/).

## How do I get set up?

To ease development and deployment, the many software dependencies are managed in a [devcontainer](.devcontainer/devcontainer.json), which in turn is built by [rocker-geospatial-dinamica](https://github.com/ethzplus/rocker-geospatial-dinamica).
This is particularly well integrated with VS Code, [see the docs](https://code.visualstudio.com/docs/devcontainers/containers).
If you prefer to work in a native `renv` environment, you're welcome to use `renv::init()` from the project's base directory, which should ask you how to discover dependencies.
You will be given a nice prompt where to read the project dependencies from.
I suggest you go with the [`DESCRIPTION`](./DESCRIPTION), since you would otherwise end up installing some seldom-used packages - you can always install those later.

## To Do

This repository is now in a state where it should be stable.
However, there is an effort going on at [evoland-plus](https://github.com/ethzplus/evoland-plus) to wrap the logic here into a more consistent format, where data is managed by a database instead of folder structures and individual files with sensitive naming schemes.
TODOs are being tracked in [this GitHub project](https://github.com/orgs/ethzplus/projects/1) and the [issues](issues) on this repository.
TODO, FIXME, and WARNING comments are used throughout the codebase; if you encounter a problem with a specific section of code, chances are that the scripts do not generalize well outside of the analytic domain they were written for.

## History of the repo

The commit history is rewritten (using [git-filter-repo](https://github.com/newren/git-filter-repo)) to omit large binary files above 1MiB and made all paths lowercase to avoid trouble with case insensitive file systems.

A bit of guesswork has gone into the reconstruction of file names and paths: occasionally, you may find a file that was referenced in some place but committed to the repository under a different name.
Where I was reasonably confident that two paths should match up, I changed either the reference or renamed the referred object.
