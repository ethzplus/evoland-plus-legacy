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

- [ ] Merge the changes from Manuel
- [ ] Merge the branches, where sensible
- [ ] Compare to the public repo version

[^filterrepo]: I used <https://github.com/newren/git-filter-repo>
