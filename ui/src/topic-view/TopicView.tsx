import { useCallback, useEffect, useRef, useState } from "react";
import Axios from 'axios'
import { useParams } from "react-router-dom";
import { Button, Card, Dimmer, Icon, Loader, Pagination, Statistic } from "semantic-ui-react";
import { KafkaEvent, Topic, TopicPartition } from "../dto";
import { JSONView } from "./JsonView";


type PartitionedEvents = { [key: number]: KafkaEvent[]}

// per partition and per page
const NUM_OF_EVENTS_PER_PAGE = 10


function getTopic(name: string): Promise<Topic> {
  return Axios.get(`/api/topics/${name}`)
    .then(({ data }) => data)
}

function numberOfEvents(topic: Topic | undefined): number {
  if (!topic) {
    return 0
  }

  return topic.partitions
    .map(p => p.latestOffset - p.earliestOffset)
    .reduce((acc, it) => acc + it, 0)
}

function numberOfPages(topic: Topic | undefined): number {
  if (!topic) {
    return 0
  }

  const minOffset = Math.min(...Object.values(topic.partitions).map(partition => partition.earliestOffset))
  const maxOffset = Math.max(...Object.values(topic.partitions).map(partition => partition.latestOffset))
  return Math.ceil((maxOffset - minOffset) / NUM_OF_EVENTS_PER_PAGE)
}

function page(topic: Topic | undefined, partitionedEvents: PartitionedEvents): number {
  if (!topic || !partitionedEvents) {
    return 0
  }

  const offsets = 
    Object.values(partitionedEvents)
      .flatMap(events => events)
      .map(event => event.offset)

  const minOffset = Math.min(...Object.values(topic.partitions).map(partition => partition.earliestOffset))
  const latestOffset = Math.max(...offsets)
  const p = Math.ceil((latestOffset - minOffset) / NUM_OF_EVENTS_PER_PAGE)
  console.log('selected page number', p)
  return p
}

function fetchEvents(topic: Topic, numberOfPages: number, page: number): Promise<KafkaEvent[]> {
  const offsetRange = (partition: TopicPartition, isLatest: boolean) => 
  Math.max(
    partition.earliestOffset,
    partition.latestOffset - (numberOfPages - page - Number(isLatest) + 1) * NUM_OF_EVENTS_PER_PAGE
)
const params = topic.partitions
  .map(partition => ({ 
    [`t0p${partition.id}e`]: offsetRange(partition, false),
    [`t0p${partition.id}l`]: offsetRange(partition, true)
  }))
  .reduce((acc: any, it: any) => ({ ...acc, ...it }), ({ t0: topic.name } as any))

return Axios.get<KafkaEvent[]>('/api/events', { params })
  .then(({ data }) => data)}

function TopicView() {
  const { topicName } = useParams();
  if (!topicName) {
    throw Error('topic param must be defined')
  }

  const eventSource = useRef<EventSource | null>(null)
  const [isStreaming, setStreaming] = useState(false)
  const [loadingMessage, setLoadingMessage] = useState<string | null>(null)
  const [events, setEvents] = useState({} as PartitionedEvents) // events per partition
  const [topic, setTopic] = useState(undefined as any as Topic)
  const addKafkaEvent = useCallback(
    (kafkaEvent: any) => {
      const event = JSON.parse(kafkaEvent) as KafkaEvent
      const { partition } = event
      const offset = event.offset + 1
      const topicPartition = topic.partitions.find(p => p.id === partition)
      if (topicPartition?.latestOffset && offset > topicPartition.latestOffset) {
        topicPartition.latestOffset = offset
        setTopic(topic)
      }

      return setEvents(prevEvents => {
        return {
          ...prevEvents,
          [event.partition]: [event, ...(prevEvents[partition] || [])].slice(0, NUM_OF_EVENTS_PER_PAGE)
        }
      })
    },
    [topic]
  ) 

  const loadEvents = (page: number) => {
    setLoadingMessage('Loading events...')
    const numOfPages = numberOfPages(topic)
    fetchEvents(topic, numOfPages, page)
      .then(kafkaEvents => {
        const partitionsEvents: PartitionedEvents = {}
        kafkaEvents.forEach(event => {
          const partition = event.partition
          partitionsEvents[partition] = [event, ...(partitionsEvents[partition] || [])]  
        })
        setEvents(partitionsEvents)
      })
      .finally(() => setLoadingMessage(null))
  }

  useEffect(() => {
    if (!topic) {
      setLoadingMessage('Loading topic metadata...')
      getTopic(topicName)
        .then(topic => setTopic(topic))
      return
    }

    if (isStreaming && !eventSource.current) {
      const params = 
        Object.entries(events)
          .map(([partition, partitionEvents]) => ({ partition, offset: 1 + Math.max(...partitionEvents.map(event => event.offset)) }))
          .reduce((acc, it) => ({...acc, [`t0p${it.partition}e`]: it.offset }), { t0: topicName } )

      console.log('params', params)
      const paramStr =
          Object.entries(params)
            .map(([key, value]) => `${key}=${value}`)
            .join('&')

      const url = process.env.NODE_ENV === "development"
        ? `http://0.0.0.0:8080/api/events?${paramStr}`
        : `/api/events?${paramStr}`

      eventSource.current = new EventSource(url) 
      eventSource.current.onmessage = (message) => addKafkaEvent(message.data)
      return () => {}
    } 
    
    if (eventSource.current) {
      eventSource.current?.close()
      eventSource.current = null
      return () => {}
    }  
    
    loadEvents(numberOfPages(topic))
    return () => {}
  // eslint-disable-next-line
  }, [topicName, topic, isStreaming])

  return (
    <>
      <Dimmer active={loadingMessage !== null} inverted>
        <Loader inverted>{ loadingMessage }</Loader>
      </Dimmer>

      <Statistic size="mini">
        <Statistic.Value>{ numberOfEvents(topic) }</Statistic.Value>
        <Statistic.Label>Events</Statistic.Label>
      </Statistic>

      <Pagination
          disabled={isStreaming}
          totalPages={numberOfPages(topic)} 
          activePage={page(topic, events)}
          onPageChange={(_, changedPage) => {
            loadEvents(Number(changedPage.activePage))
          }}
      />
      <Button icon basic onClick={() => setStreaming(!isStreaming)}>
        <Icon name={isStreaming ? 'pause' : 'play'} />
      </Button>
      
      <Card.Group>
        {
          Object.values(events)
            .flatMap(events => events)
            .sort((a, b) => b.timestamp - a.timestamp).map((e) => 
              <Card key={e.partition + '' + e.offset} fluid>
                <Card.Content>
                  <JSONView event={e.key}/>
                </Card.Content>

                <Card.Content>
                  <Card.Description>
                    <JSONView event={e.value}/>
                  </Card.Description>
                </Card.Content>


                <Card.Content extra>
                    <Card.Meta content={`parittion: ${e.partition} | offset: ${e.offset} | timestamp: ${new Date(e.timestamp).toISOString()}`} />
                  </Card.Content>
              </Card>
          )
        }
      </Card.Group>
    </>
  );
}

  
export default TopicView;