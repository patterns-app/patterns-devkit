![basis](https://github.com/basis-os/basis/workflows/basis/badge.svg)

<p>&nbsp;</p>
<p align="center">
  <img width="150" src="assets/logo.svg">
</p>
<p>&nbsp;</p>
<p align="center">
  <em>Basis - Build data workflows from re-usable sql and python components</em>
</p>

---

Basis is a tool for building data and analytics systems. It makes development faster and easier by isolating data operations into functional components that reactively execute on a computational node graph (DAG).

Common use cases for Basis are:

- Business process automation
- Data integration, ETL, business intelligence, and ad hoc analytics
- Business systems data synchronization
- Predictive analytics, dynamic pricing engines, and decision automation

### Core Concepts

- **Repository** - a file directory that contains dataspace, app and component files and provides a top-level container for organizing work (commonly sync'd with a GitHub repo).
- **Dataspace -** a database and cloud compute environment for nodes + apps that is defined by a dataspace.yml configuration file — eg. production, development, testing.
- **Component** - a function that operates on input data and produces outputs. It is defined by a file written in python or sql that is deployed via an app as a node to a dataspace.
- **App** - a configuration file that defines a list of components, parameters and settings that together define how nodes operate in a dataspace to create a computational DAG.
- **Node** - a container in a dataspace that is programmed by a component and provides compute and database resources to process inputs and store outputs, state, and errors
- **Protocol** - a set of APIs that components use to interact with nodes

## Installation

`pip install basis-cli`

## Usage

In a new git repository:

`basis generate new`

This will create a skeleton for a basis project:

```
dataspace.yml
README.md
requirements.txt
```

`basis generate app myapp`

To create a new data app:

```
dataspace.yml
myapp/
  app.yml
README.md
requirements.txt
```

`basis generate component myapp/component.py --simple`

```
dataspace.yml
myapp/
  app.yml
  component.py
README.md
requirements.txt
```

And then edit the app configuration `myapp/app.yml` to specify node configuration:

```yaml
name: myapp
nodes:
  - name: stripe_node
    component: basis_modules.stripe.extract_stripe
  - name: mynode
    component: component.yml
    inputs: stripe_node
```

## Deploy

To deploy a dataspace, edit your `dataspace.yml` to specify the apps to deploy:

```yaml
name: mydataspace
basis:
  version: 0.1.0
apps:
  - app: myapp/app.yml
```

And then run:

`basis deploy dataspace.yml`
