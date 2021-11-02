# Quad YAML format
Named "quad" because there are 4 main elements:
1. graph.yml -> node config for the entire graph
2. connections.yml -> node declarations for the entire graph 
3. subgraph.yml -> optional, if any, declar subgraphs and their interfaces here
4. nodes - folder containing all node scripts that exist as standalone sql or py files. They can be organized in folders or in one main folder, doesn't matter. 

# Features:
### Separation of concerns that match developer experience = easy to learn / repeat / copy pasta with minimal redundancy
    1. Write node code: declare what inputs you'll use, write some computation, declare the output
    2. Go to graph.yml, configure your node (set params, name outputs, give it labels, a description etc.)
    3. Go to connections.yml, connect er up

### Universal node connection references (is this possible?):
In all files where a node output is references it follows this format:
`table(node.output)`
or
`stream(node.output)`
for example: 
`table(import_transactions.raw_transactions)`

### Graph.yml - Read-ability, type-ability
Graph.yml utilizes the yaml document separator "---" to list nodes. This enables a user to create easy visual cue's for separating notes, and allows them to avoid concerns with intentation. This make the document easier to both read and write. 

### How graph.yml and connections.yml work with subgraph.yml
Together, these two configuration files, describe a flat graph with no heirarchy or subgraphs. Given the user experience will typically consist of deveoping a pipeline, then only afterwards grouping nodes, this structure enables a user to layer a subgraph onto their exisitng graph without needing to delete or change anything about their prior setup. 
