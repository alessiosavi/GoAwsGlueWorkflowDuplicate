package main

import (
	"context"
	"encoding/json"
	"flag"
	awsutils "github.com/alessiosavi/GoGPUtils/aws"
	fileutils "github.com/alessiosavi/GoGPUtils/files"
	stringutils "github.com/alessiosavi/GoGPUtils/string"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"io/ioutil"
	"log"
	"strings"
)

type conf struct {
	WorkflowName         string            `json:"workflow_name"`
	WorkflowRegion       string            `json:"workflow_region"`
	WorkflowTargetRegion string            `json:"workflow_target_region"`
	Replacer             map[string]string `json:"replacer"`
}

func (c *conf) Validate() {
	if stringutils.IsBlank(c.WorkflowName) {
		panic("workflow_name parameter not provided")
	}

}
func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Llongfile)

	confFile := flag.String("conf", "", "Path of the configuration file")
	flag.Parse()
	if stringutils.IsBlank(*confFile) {
		flag.Usage()
		panic("-conf parameter not provided")
	}
	if !fileutils.IsFile(*confFile) {
		panic("File [" + *confFile + "] not found!")
	}

	data, err := ioutil.ReadFile(*confFile)
	if err != nil {
		panic(err)
	}
	var configuration conf

	if err = json.Unmarshal(data, &configuration); err != nil {
		panic(err)
	}

	if indent, err := json.MarshalIndent(configuration, " ", "  "); err == nil {
		log.Println("Using the following configuration:\n" + string(indent))
	}

	copyWorkflow(configuration)

}

func copyWorkflow(conf conf) {
	cfg, err := awsutils.New()
	if err != nil {
		panic(err)
	}
	glueBaseRegionClient := glue.New(glue.Options{Credentials: cfg.Credentials, Region: conf.WorkflowRegion})
	glueTargetRegionClient := glue.New(glue.Options{Credentials: cfg.Credentials, Region: conf.WorkflowTargetRegion})

	// Set the string that have to be replaced during the creation of the new workflow
	var toReplace []string
	for k, v := range conf.Replacer {
		toReplace = append(toReplace, k)
		toReplace = append(toReplace, v)
	}
	replacer := strings.NewReplacer(toReplace...)
	workflows, err := glueTargetRegionClient.ListWorkflows(context.Background(), &glue.ListWorkflowsInput{})
	if err != nil {
		panic(err)
	}
	// Remove the workflow if already exists
	for _, workflow := range workflows.Workflows {
		if workflow == replacer.Replace(conf.WorkflowName) {
			getWorkflow, err := glueTargetRegionClient.GetWorkflow(context.Background(), &glue.GetWorkflowInput{
				Name:         aws.String(workflow),
				IncludeGraph: aws.Bool(true),
			})
			if err != nil {
				panic(err)
			}
			for _, node := range getWorkflow.Workflow.Graph.Nodes {
				if node.TriggerDetails != nil && node.TriggerDetails.Trigger != nil {
					if _, err := glueTargetRegionClient.DeleteTrigger(context.Background(), &glue.DeleteTriggerInput{Name: node.TriggerDetails.Trigger.Name}); err != nil {
						panic(err)
					}
				}

			}
			_, err = glueTargetRegionClient.DeleteWorkflow(context.Background(), &glue.DeleteWorkflowInput{
				Name: aws.String(workflow),
			})
			if err != nil {
				panic(err)
			}
		}
	}

	// Retrieve the workflow that have to be copied
	workflow, err := glueBaseRegionClient.GetWorkflow(context.Background(), &glue.GetWorkflowInput{
		Name:         aws.String(conf.WorkflowName),
		IncludeGraph: aws.Bool(true),
	})

	if err != nil {
		panic(err)
	}
	// Create a new workflow with the same properties
	workflowName := aws.String(replacer.Replace(conf.WorkflowName))
	_, err = glueTargetRegionClient.CreateWorkflow(context.Background(), &glue.CreateWorkflowInput{
		Name:                 workflowName,
		DefaultRunProperties: workflow.Workflow.DefaultRunProperties,
		Description:          workflow.Workflow.Description,
		MaxConcurrentRuns:    workflow.Workflow.MaxConcurrentRuns,
	})
	if err != nil {
		panic(err)
	}

	nodes := workflow.Workflow.Graph.Nodes
	for _, node := range nodes {
		node.Name = aws.String(replacer.Replace(*node.Name))
		log.Println("Name: " + *node.Name)

		if node.TriggerDetails != nil && node.TriggerDetails.Trigger != nil {
			node.TriggerDetails.Trigger.WorkflowName = workflowName

			if len(node.TriggerDetails.Trigger.Actions) > 0 {
				node.TriggerDetails.Trigger.Name = aws.String(replacer.Replace(*node.TriggerDetails.Trigger.Name))
				for i := range node.TriggerDetails.Trigger.Actions {
					if node.TriggerDetails.Trigger.Actions[i].JobName != nil {
						jobName := replacer.Replace(*node.TriggerDetails.Trigger.Actions[i].JobName)
						// NOTE: In case of job does not exists, the (obviously not working) workflow will be created without issue
						node.TriggerDetails.Trigger.Actions[i].JobName = aws.String(replacer.Replace(jobName))
					}
					if node.TriggerDetails.Trigger.Actions[i].CrawlerName != nil {
						// NOTE: In case of crawler does not exists, the (obviously not working) workflow will be created without issue
						node.TriggerDetails.Trigger.Actions[i].CrawlerName = aws.String(replacer.Replace(*node.TriggerDetails.Trigger.Actions[i].CrawlerName))
					}
				}
			}

			if node.TriggerDetails.Trigger.Predicate != nil && len(node.TriggerDetails.Trigger.Predicate.Conditions) > 0 {
				for i := range node.TriggerDetails.Trigger.Predicate.Conditions {
					if node.TriggerDetails.Trigger.Predicate.Conditions[i].JobName != nil {
						jobName := replacer.Replace(*node.TriggerDetails.Trigger.Predicate.Conditions[i].JobName)
						// NOTE: In case of job does not exists, the (obviously not working) workflow will be created without issue
						node.TriggerDetails.Trigger.Predicate.Conditions[i].JobName = aws.String(replacer.Replace(jobName))
					}
					if node.TriggerDetails.Trigger.Predicate.Conditions[i].CrawlerName != nil {
						// NOTE: In case of crawler does not exists, the (obviously not working) workflow will be created without issue
						node.TriggerDetails.Trigger.Predicate.Conditions[i].CrawlerName = aws.String(replacer.Replace(*node.TriggerDetails.Trigger.Predicate.Conditions[i].CrawlerName))
					}
				}
			}

			//marshalIndent, err := json.MarshalIndent(node, " ", "  ")
			//if err != nil {
			//	panic(err)
			//}
			//log.Println(string(marshalIndent))

			var start bool = false
			if node.TriggerDetails.Trigger.Type != gluetypes.TriggerTypeOnDemand {
				start = true
			}
			if _, err = glueTargetRegionClient.CreateTrigger(context.Background(), &glue.CreateTriggerInput{
				Actions:         node.TriggerDetails.Trigger.Actions,
				Name:            node.TriggerDetails.Trigger.Name,
				Type:            node.TriggerDetails.Trigger.Type,
				Description:     node.TriggerDetails.Trigger.Description,
				Predicate:       node.TriggerDetails.Trigger.Predicate,
				Schedule:        node.TriggerDetails.Trigger.Schedule,
				StartOnCreation: start,
				Tags:            nil,
				WorkflowName:    workflowName}); err != nil {
				panic(err)
			}
		}
	}
}
