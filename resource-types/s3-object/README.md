# AWSCommunity::S3::Object

- [Overview](#Overview)

- [Usage](#Usage)

- [Tests](#Tests)

  - [Unit tests](#Unit-tests)

  - [Contract tests](#Contract-tests)

- [Example schema and handlers](#Example-schema-and-handlers)

  - [Type hints](#Type-hints)


## Overview
This is an example resource type for [AWS CloudFormation](https://aws.amazon.com/cloudformation/) that describes an S3 Object.  For more information on creating resource types, see [Creating resource types](https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-types.html) in the CloudFormation Command Line Interface documentation.


## Usage
For more information on syntax usage for this example resource type, see the [docs/README.md](docs/README.md) page.

If you choose to activate and test this example resource type in your AWS account:

- install and configure the [CloudFormation Command Line Interface (CLI)](https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/what-is-cloudformation-cli.html)
- clone this repository on your workstation
- on your workstation, change directory to the directory where this `README.md` file is located
- register the example resource type with CloudFormation as a private extension in the AWS account and region where you want to use the resource.  As part of this process, you leverage CloudFormation to describe and create, in your account, an [AWS Identity and Access Management](https://aws.amazon.com/iam/) (IAM) role, that is assumed by CloudFormation when _Create, Read, Update, Delete, List_ (CRUDL) operations occur to manage the resource on your behalf.  The IAM role policy will include actions specified in the [handlers](https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-type-schema.html#schema-properties-handlers) section of the JSON schema file for the resource.  In this example, the _handlers_ section of the `aws-s3-object.json` schema file describes actions such as `s3:PutObject`, `s3:GetObject`, `s3:ListObjects`, `s3:DeleteObject` where applicable in relevant `create`, `read`, `update`, `delete`, and `list` handlers
  - choose to use the [submit](https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-type-cli-submit.html) command of the [CloudFormation CLI](https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/what-is-cloudformation-cli.html) to register the example resource type, e.g.: `cfn generate && cfn submit --set-default --region REGION_YOU_WISH_TO_USE`
  - verify the example resource type is registered in the account and region you chose: navigate to the AWS CloudFormation console, and from _Registry_ choose _Activated extensions_; you should find the `AWS::S3::Object` resource type in the _Resource types_ list
  - for more information, see [register private extensions](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/registry-register.html)


## Tests
Example unit tests and information on how to run unit and contract tests are shown next.


### Contract tests
Contract tests help you validate the resource type you're developing works as you expect.  For more information, see [Testing resource types using contract tests](https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-type-test.html) in the AWS documentation.  You use the [test](https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-type-cli-test.html) command of the CloudFormation CLI to run contract tests in your account: in this example, you will specify example values to create, update, delete S3 Objects in your account and region.  For contract tests runs, you can choose to specify an execution role that contract tests can assume; alternatively, contract tests will use your environment credentials or credentials specified in the Boto3 credentials chain.

When you run contract tests, you pass in input values; for more information, see [Specifying input data for use in contract tests](https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-type-test.html#resource-type-test-input-data).  Contract tests for this example resource type use `create`, `update` and `invalid` input data from files in the `inputs` directory.  If you inspect create- and update-related input files content in the aforementioned directory, you will see a line such as `"S3BucketName": "{{S3BucketNameForContractTests}}",`: this line is used by contract tests to take, as an input, the public key material that you will use to run contract tests, and input data in this case is taken from a CloudFormation stack you will need to create before running contract tests.  To run contract tests for this resource type:

- create a CloudFormation stack using the `examples/example-template-contract-tests-input.yaml` template. In the `Outputs` section of this stack, you will find an `S3BucketName` output exported as `S3BucketNameForContractTests`, which is the value contract tests in this example will read
- run the Local Lambda Service: `sam local start-lambda`; for more information, see [Testing resource types locally using SAM](https://docs.aws.amazon.com/cloudformation-cli/latest/userguide/resource-type-test.html#resource-type-develop-test)
- run contract tests as follows: `cfn generate && cfn submit --dry-run && cfn test --region REGION_YOU_WISH_TO_USE`
- when you are done running contract tests/submitting the module, you can choose to delete the stack you created as part of this contract tests section