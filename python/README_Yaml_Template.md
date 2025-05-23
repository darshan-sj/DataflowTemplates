
YAML template
---
The YAML Template is used to run Dataflow pipelines written in Beam YAML. The
YAML pipeline can be passed to the template directly as a raw string or the
location of a Beam YAML pipeline file stored in Google Cloud Storage can
optionally be passed.

For launching a Beam YAML pipeline directly from the gcloud command line, see
https://cloud.google.com/sdk/gcloud/reference/dataflow/yaml.

For more information on Beam YAML, see
https://beam.apache.org/documentation/sdks/yaml/.



:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters


### Optional parameters

* **yaml_pipeline**: A yaml description of the pipeline to run.
* **yaml_pipeline_file**: A file in Cloud Storage containing a yaml description of the pipeline to run.
* **jinja_variables**: A json dict of variables used when invoking the jinja preprocessor on the provided yaml pipeline.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=python/src/main/java/com/google/cloud/teleport/templates/python/YAMLTemplate.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin).

### Building Template

This template is a Flex Template, meaning that the pipeline code will be
containerized and the container will be executed on Dataflow. Please
check [Use Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates)
and [Configure Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates)
for more information.

#### Staging the Template

If the plan is to just stage the template (i.e., make it available to use) by
the `gcloud` command or Dataflow "Create job from template" UI,
the `-PtemplatesStage` profile should be used:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>

mvn clean package -PtemplatesStage  \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-DstagePrefix="templates" \
-DtemplateName="Yaml_Template" \
-f python
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Yaml_Template
```

The specific path should be copied as it will be used in the following steps.

#### Running the Template

**Using the staged template**:

You can use the path above run the template (or share with others for execution).

To start a job with the template at any time using `gcloud`, you are going to
need valid resources for the required parameters.

Provided that, the following command line can be used:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Yaml_Template"

### Required

### Optional
export YAML_PIPELINE=<yaml_pipeline>
export YAML_PIPELINE_FILE=<yaml_pipeline_file>
export JINJA_VARIABLES=<jinja_variables>

gcloud dataflow flex-template run "yaml-template-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "yaml_pipeline=$YAML_PIPELINE" \
  --parameters "yaml_pipeline_file=$YAML_PIPELINE_FILE" \
  --parameters "jinja_variables=$JINJA_VARIABLES"
```

For more information about the command, please check:
https://cloud.google.com/sdk/gcloud/reference/dataflow/flex-template/run


**Using the plugin**:

Instead of just generating the template in the folder, it is possible to stage
and run the template in a single command. This may be useful for testing when
changing the templates.

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

### Required

### Optional
export YAML_PIPELINE=<yaml_pipeline>
export YAML_PIPELINE_FILE=<yaml_pipeline_file>
export JINJA_VARIABLES=<jinja_variables>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="yaml-template-job" \
-DtemplateName="Yaml_Template" \
-Dparameters="yaml_pipeline=$YAML_PIPELINE,yaml_pipeline_file=$YAML_PIPELINE_FILE,jinja_variables=$JINJA_VARIABLES" \
-f python
```

## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
see [dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job).

Terraform modules have been generated for most templates in this repository. This includes the relevant parameters
specific to the template. If available, they may be used instead of
[dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job)
directly.

To use the autogenerated module, execute the standard
[terraform workflow](https://developer.hashicorp.com/terraform/intro/core-workflow):

```shell
cd v2/python/terraform/Yaml_Template
terraform init
terraform apply
```

To use
[dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job)
directly:

```terraform
provider "google-beta" {
  project = var.project
}
variable "project" {
  default = "<my-project>"
}
variable "region" {
  default = "us-central1"
}

resource "google_dataflow_flex_template_job" "yaml_template" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Yaml_Template"
  name              = "yaml-template"
  region            = var.region
  parameters        = {
    # yaml_pipeline = "<yaml_pipeline>"
    # yaml_pipeline_file = "<yaml_pipeline_file>"
    # jinja_variables = "<jinja_variables>"
  }
}
```
