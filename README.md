<p>&nbsp;</p>
<p align="center">
  <img width="150" src="assets/logo.svg">
</p>
<p>&nbsp;</p>
<p align="center">
  <em>Basis - Build data systems from re-usable sql and python components</em>
</p>

---

## Installation

`pip install basis-devkit`

## Usage

In a new git repository:

`basis generate graph mygraph`

This will create a skeleton for a basis graph:

```
mygraph/
  graph.yml
  README.md
  requirements.txt
```

To create a simple python node template:

`basis generate node mygraph/mynode.py --simple`

```
mygraph/
  graph.yml
  mynode.py
  README.md
  requirements.txt
```

And then edit the graph configuration `mygraph/graph.yml` to specify node configuration:

```yaml
name: mygraph
nodes:
  - python: mynode.py
    name: mynode1 # Override the default name (mynode)
```

## Deploy

To deploy a graph, you must sign up for a getbasis.com account and login to authenticate the cli:

`basis login`

Ensure you have an environment created:

`basis env list`

And then you can deploy a graph to an environment:

`basis deploy mygrpah/graph.yml -e production`
