![basis](https://github.com/basis-os/basis/workflows/basis/badge.svg)

<p>&nbsp;</p>
<p align="center">
  <img width="200" src="assets/logo.svg">
</p>
<h1 align="center">Basis</h3>
<h3 align="center">Build data workflows from re-usable sql and python components</h3>
<p>&nbsp;</p>

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
