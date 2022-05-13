<p>&nbsp;</p>
<p align="center">
  <img width="150" src="assets/logo.svg">
</p>
<p>&nbsp;</p>
<p align="center">
  <em>Patterns - Build data systems from re-usable sql and python components</em>
</p>

---

## Installation

`pip install patterns-devkit`

## Usage

`patterns create graph mygraph`

This will create an empty patterns graph:

```
mygraph/
  graph.yml
```

Create a new python node:

```
cd mygraph
patterns create node mynode.py
```

```
mygraph/
  graph.yml
  mynode.py
```

## Deploy

To deploy a graph, you must sign up for a patterns.app account and login to authenticate
the cli:

`patterns login`

Then you can deploy you graph:

`patterns upload`

## Other commands

You can see the full list of available cli commands:

```
patterns --help
```
