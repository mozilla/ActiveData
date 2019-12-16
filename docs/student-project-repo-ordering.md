

# Student Project - Define Partial Order on Repository Graphs 

## Background

A code repository contains many changesets, each with some number of parents. Together, they make up an acyclic directed graph (DAG); with branches, chains, and merges. 

Some code repositories are large, and it would be nice to extract a subset of changesets for our graph algorithms to work on. It turns out, the properties of a code repository graph allow us to simplify graph operations, and reduce graph analysis to a small region of the whole. 


## Use Cases

**Tracking performance regressions** - these regressions are caused by some changeset on one branch, and propagates to other branches when merged. From the point of another branch, the merge commit is the cause of the regression, when really it is some distant ancestor that is the true culprit. 
 
**Tracking intermittent failures** - the same logic applies to changesets that introduce intermittent failures. The logic used to detect the intermittent failure is more complicated than in performance, but the need for tracing backward through multiple merges from multiple branches is the same.

**Code Coverage** - Code coverage is expensive to run, so can't be run on every changeset.  Instead, we aggregate coverage over a number of changesets to get virtual coverage. This can be done, even on files hat have changed. This requires we track coverage diffs through merges, possibly over long periods of branch separation. 

## Solution

Define a function `N` that acts on every changeset to provide a floating point number `n = N(C)`, such that for all ancestors `Ai` and all descendants `Di` we have `N(Ai) < n < N(Di)`.  This can be done efficiently on repository graphs; a Dewey decimal like numbering system will work fine.

For every changeset, `C`, find the "supremum",`sup(C)`, and "infimum", `inf(C)`, nodes. The supremum is [the dominator](https://en.wikipedia.org/wiki/Dominator_(graph_theory)) when edges point from parent to child.  The infimum is the dominator when edges point from child to parent.  Using these nodes, For every changeset `C`, we can define three regions of the repository graph:  

1. revisions where `C` has been applied (`all Di where N(Di) >= N(inf(c))`)
2. Revisions where `C` has not been applied (`all Ai where N(Ai) <= N(sup(c))`)
3. the revisions which graph analysis is required (`all Ri where N(sup(C)) < N(R) < N(inf(C))`)

With these features, we can put complex repository graphs into a regular database, and use then to pull subsets of the graph for detailed local graph anaylsis.
