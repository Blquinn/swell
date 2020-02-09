package me.blq.swell

import me.blq.swell.workflow.Task

/*
// Broker - a common interface for all brokers
type Broker interface {
	GetConfig() *config.Config
	SetRegisteredTaskNames(names []string)
	IsTaskRegistered(name string) bool
	StartConsuming(consumerTag string, concurrency int, p TaskProcessor) (bool, error)
	StopConsuming()
	Publish(ctx context.Context, task *tasks.Signature) error
	GetPendingTasks(queue string) ([]*tasks.Signature, error)
	GetDelayedTasks() ([]*tasks.Signature, error)
	AdjustRoutingKey(s *tasks.Signature)
}
*/

interface Broker {
    fun startConsuming()
    fun stopConsuming()
    fun publish(task: Task)
//    fun getDelayedTasks(): List<DecodeSignature>
}
