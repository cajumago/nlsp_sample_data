# nlsp_pipeline

**`resources`** folder defines all source code for the 'nlsp' pipeline:

- `explorations`: Ad-hoc notebooks used to explore the data processed by this pipeline. --*Exclude as pipeline source code*
- `transformations`: All dataset definitions and transformations.
- `tests`: Unit tests and/or integration tests implementation. --*Exclude as pipeline source code*
- `utilities`: Utility functions and Python modules used in this pipeline. --*Exclude as pipeline source code*

## Getting Started

To get started, go to the `transformations` folder -- most of the relevant source code lives there:

* By convention, every dataset under `transformations` is in a separate file.
* To get familiar with the syntax read more about the syntax at https://docs.databricks.com/dlt/python-ref.html.
* Use `Run file` to run and preview a single transformation.
* Use `Dry run` to check whether a pipeline's source code is valid without running a full update.
* Use `Run pipeline` to run _all_ transformations in the entire pipeline.
* Use `+ Add` in the file browser to add a new data set definition.
* Use `Schedule` to run the pipeline on a schedule!

For more tutorials and reference material, see https://docs.databricks.com/aws/en/ldp.

## How to create and deploy a DBA in Databricks UI

1. [Set up Databricks Git folders](https://docs.databricks.com/aws/en/repos/repos-setup)
2. Under our workspace’s user `Workspace/Users/<user_name>/`, click on **Create**, then on **Asset bundle**. Follow the instructions.
3. It creates a folder with the name we provide plus `_pipeline` with all template folders and files for the Lakeflow SDP.
