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

`basis create graph mygraph`

This will create an empty basis graph:

```
mygraph/
  graph.yml
```

Create a new python node:

```
cd mygraph
basis create node mynode.py
```

```
mygraph/
  graph.yml
  mynode.py
```

## Deploy

To deploy a graph, you must sign up for a getbasis.com account and login to authenticate
the cli:

`basis login`

Then you can deploy you graph:

`basis upload`

## Other commands

You can see the full list of available cli commands:

```
basis --help
```
