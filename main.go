package main

import (
	"context"
	"encoding/json"
	"flag"
	fileutils "github.com/alessiosavi/GoGPUtils/files"
	stringutils "github.com/alessiosavi/GoGPUtils/string"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"io/ioutil"
	"log"
	"strings"
)

type conf struct {
	WorkflowName string            `json:"workflow_name"`
	Prefix       string            `json:"prefix"`
	Replacer     map[string]string `json:"replacer"`
}

func (c *conf) Validate() {
	if stringutils.IsBlank(c.WorkflowName) {
		panic("workflow_name parameter not provided")
	}
	if stringutils.IsBlank(c.Prefix) {
		panic("prefix parameter not provided")
	}
}
func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Llongfile)

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic(err)
	}
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

	glueClient := glue.New(glue.Options{Credentials: cfg.Credentials, Region: cfg.Region})

	copyWorkflow(glueClient, configuration)

}

func copyWorkflow(glueConnection *glue.Client, conf conf) {
	workflows, err := glueConnection.ListWorkflows(context.Background(), &glue.ListWorkflowsInput{})
	if err != nil {
		panic(err)
	}
	// Remove all the workflow that start with the given prefix
	for _, workflow := range workflows.Workflows {
		if strings.HasPrefix(workflow, conf.Prefix) {
			_, err = glueConnection.DeleteWorkflow(context.Background(), &glue.DeleteWorkflowInput{
				Name: aws.String(workflow),
			})
			if err != nil {
				panic(err)
			}
		}
	}

	// Retrieve the workflow that have to be copied
	workflow, err := glueConnection.GetWorkflow(context.Background(), &glue.GetWorkflowInput{
		Name:         aws.String(conf.WorkflowName),
		IncludeGraph: aws.Bool(true),
	})
	if err != nil {
		panic(err)
	}
	// Create a new workflow with the same properties
	_, err = glueConnection.CreateWorkflow(context.Background(), &glue.CreateWorkflowInput{
		Name:                 aws.String(conf.Prefix + conf.WorkflowName),
		DefaultRunProperties: workflow.Workflow.DefaultRunProperties,
		Description:          workflow.Workflow.Description,
		MaxConcurrentRuns:    workflow.Workflow.MaxConcurrentRuns,
	})
	if err != nil {
		panic(err)
	}

	// Set the string that have to be replaced during the creation of the new workflow
	var toReplace []string
	for k, v := range conf.Replacer {
		toReplace = append(toReplace, k)
		toReplace = append(toReplace, v)
	}
	replacer := strings.NewReplacer(toReplace...)

	nodes := workflow.Workflow.Graph.Nodes
	for _, node := range nodes {
		log.Println("Name: " + *node.Name)
		node.Name = aws.String(conf.Prefix + *node.Name)
		if node.TriggerDetails != nil && node.TriggerDetails.Trigger != nil {
			node.TriggerDetails.Trigger.WorkflowName = aws.String(conf.Prefix + conf.WorkflowName)

			if len(node.TriggerDetails.Trigger.Actions) > 0 {
				node.TriggerDetails.Trigger.Name = aws.String(conf.Prefix + *node.TriggerDetails.Trigger.Name)
				for i := range node.TriggerDetails.Trigger.Actions {
					if node.TriggerDetails.Trigger.Actions[i].JobName != nil {
						jobName := replacer.Replace(*node.TriggerDetails.Trigger.Actions[i].JobName)
						// NOTE: In case of job does not exists, the (obviously not working) workflow will be created without issue
						node.TriggerDetails.Trigger.Actions[i].JobName = aws.String(conf.Prefix + jobName)
					}
					if node.TriggerDetails.Trigger.Actions[i].CrawlerName != nil {
						// NOTE: In case of crawler does not exists, the (obviously not working) workflow will be created without issue
						node.TriggerDetails.Trigger.Actions[i].CrawlerName = aws.String(conf.Prefix + *node.TriggerDetails.Trigger.Actions[i].CrawlerName)
					}
				}
			}

			if node.TriggerDetails.Trigger.Predicate != nil && len(node.TriggerDetails.Trigger.Predicate.Conditions) > 0 {
				for i := range node.TriggerDetails.Trigger.Predicate.Conditions {
					if node.TriggerDetails.Trigger.Predicate.Conditions[i].JobName != nil {
						jobName := replacer.Replace(*node.TriggerDetails.Trigger.Predicate.Conditions[i].JobName)
						// NOTE: In case of job does not exists, the (obviously not working) workflow will be created without issue
						node.TriggerDetails.Trigger.Predicate.Conditions[i].JobName = aws.String(conf.Prefix + jobName)
					}
					if node.TriggerDetails.Trigger.Predicate.Conditions[i].CrawlerName != nil {
						// NOTE: In case of crawler does not exists, the (obviously not working) workflow will be created without issue
						node.TriggerDetails.Trigger.Predicate.Conditions[i].CrawlerName = aws.String(conf.Prefix + *node.TriggerDetails.Trigger.Predicate.Conditions[i].CrawlerName)
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
			if _, err = glueConnection.CreateTrigger(context.Background(), &glue.CreateTriggerInput{
				Actions:         node.TriggerDetails.Trigger.Actions,
				Name:            node.TriggerDetails.Trigger.Name,
				Type:            node.TriggerDetails.Trigger.Type,
				Description:     node.TriggerDetails.Trigger.Description,
				Predicate:       node.TriggerDetails.Trigger.Predicate,
				Schedule:        node.TriggerDetails.Trigger.Schedule,
				StartOnCreation: start,
				Tags:            nil,
				WorkflowName:    node.TriggerDetails.Trigger.WorkflowName}); err != nil {
				panic(err)
			}
		}
	}
}
