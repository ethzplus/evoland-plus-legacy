# Project Overview

This repo is set up by @mmyrte to gather the different incarnations of LULCC-CH as a preparatory step for a more long-term evoland-plus software. This repo is based on the original history of [LULCC-CH-private](https://github.com/blenback/LULCC-CH-private).

I have rewritten the commit history to omit large binary files above 1MiB[^filterrepo], which weren't many, but added up to the repo being 200MiB (or 96MiB bare). We can always add them back; I suppose that `Exemplar_entroy_table.rds` might be the only relevant one.

```sh
~/github-repos/blenback/LULCC-CH-private find . -type f -exec du -h {} + | sort -rh | head -7
 96M ./.git/objects/pack/pack-780aa1ad53bc5398b12dba43c97d2329e092c2c5.pack
 54M ./Scripts/Testing_scripts/Demo_prob_predictions.rds
 54M ./Scripts/Testing_scripts/Demo_prob_predictions (restored).rds
2.9M ./Scripts/Testing_scripts/Exemplar_entropy_table.rds
2.7M ./Scripts/Testing_scripts/Tree_prob_predictions.rds
772K ./Tools/Transition_Tables.xlsx
 84K ./Model/Dinamica_models/LULCC_CH_simulation_2023-07-20.ego
```

I've also rewritten all files to be lowercase, because some commits were probably made on a case-insensitive filesystem. Git apparently didn't track the implied renames, hence leading to a very weird repository state, where a change would both be real and unreal, at least on macOS. We might have to rewrite some file paths in the scripts.

# Todos

- [x] Merge the changes from Manuel
    - I've merged some changes and tried to leave in as many of @blenback's newer comments as possible. It's very likely that I broke stuff, because the divergence was so far gone that I didn't want to have to run everything anew.
- [x] Merge the branches, where sensible
    - I don't think it makes sense to merge the HPC branch now (2025-01-28); I'll focus on getting a grip on the functionality of the current main branch instead. Can always check back later.
- [x] Compare to the public repo version
    - I did a rough comparison of [this commit on `blenback/LULCC-CH`](https://github.com/blenback/LULCC-CH/tree/0bb2b0c59660270622878837620fe868b1854dba) and [this commit on this repo](https://github.com/ethzplus/evoland-with-baggage/tree/5d5f62d2d2783e3a0500d872df38037e8c0255c8) and as far as I could discern, we're only missing out on small bugfixes. It would be a major hassle to try and merge, so the latter commit is from where I'll start writing tests and refactoring.
- [ ] Make R code testable; i.e. introduce [testthat](https://testthat.r-lib.org)
    - Also implies adding automated data fetchers where possible. If not, add small file excerpts into the testing structure and try to find out where they came from.

[^filterrepo]: I used <https://github.com/newren/git-filter-repo>
