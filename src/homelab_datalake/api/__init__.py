"""The HTTP surface over the lake: what it holds, and how to ask it a question.

The MCP server next door answers *typed* questions -- a model picks a dataset, a
grain and a measure, and the semantic layer builds the SQL. That is the right shape
for a model and the wrong shape for a person, who arrives already knowing what they
want and has to translate it into the vocabulary of somebody else's registry.

This package is the other half: the whole schema, and arbitrary SQL over it. Which
means every guard the semantic layer got for free by construction has to be built
explicitly here, and that is why they live in Python rather than in the web client
that calls it. A limit a caller can choose is not a limit; a limit in a TypeScript
file the browser downloads is a suggestion. The parser, the row cap, the deadline
and the audit log are all on this side of the network, and are tested with the rest
of the repository.

Two modules, and the split is deliberate:

    catalogue.py  what exists -- joined from Parquet footers and the dbt manifest
    query.py      what a caller may run, and under which limits
    audit.py      what was run, by whom, and what came back
    app.py        the routes, and the bearer token in front of them
"""
