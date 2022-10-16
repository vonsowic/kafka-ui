import { useCallback, useEffect, useRef, useState } from "react";
import Axios from 'axios'
import { useParams } from "react-router-dom";
import { Button, Card, Dimmer, Divider, Icon, Loader, Pagination } from "semantic-ui-react";
import { KafkaEvent, Topic, TopicPartition } from "../dto";
import { JSONView } from "./JsonView";


type PartitionedEvents = { [key: number]: Array<KafkaEvent>}

// per partition and per page
const NUM_OF_EVENTS_PER_PAGE = 10


function getTopic(name: string): Promise<Topic> {
  return Axios.get(`/api/topics/${name}`)
    .then(({ data }) => data)
}

function numberOfPages(topic: Topic | undefined): number {
  if (!topic) {
    return 0
  }

  const numbersOfRecors = topic.partitions.map(partition => partition.latestOffset - partition.earliestOffset)
  return Math.ceil(Math.max(...numbersOfRecors) / NUM_OF_EVENTS_PER_PAGE)
}


function TopicView() {
  const { topicName } = useParams();
  if (!topicName) {
    throw Error('topic param must be defined')
  }

  const eventSource = useRef<EventSource | null>(null)
  const [isStreaming, setStreaming] = useState(false)
  const [loadingMessage, setLoadingMessage] = useState<string | null>(null)
  const [page, setPage] = useState(0)
  const [events, setEvents] = useState({} as PartitionedEvents) // events per partition
  const [topic, setTopic] = useState(undefined as any as Topic)
  const addKafkaEvent = useCallback(
    // TODO: update topic state
    (kafkaEvent: any) => {
      const event = JSON.parse(kafkaEvent) as KafkaEvent
      const partition = event.partition
      console.log('sse event', event)
      return setEvents(prevEvents => {
        console.log('prevEvents', prevEvents)
        return {
        // ...prevEvents,
        [event.partition]: [event, ...(prevEvents[partition] || [])].slice(0, NUM_OF_EVENTS_PER_PAGE)
        }
      })
    },
    []
  ) 

  useEffect(() => {
    if (!topic) {
      setLoadingMessage('Loading topic metadata...')
      getTopic(topicName)
        .then(topic => {
          setPage(numberOfPages(topic))
          setTopic(topic)
        })
      return
    }

    const offsetRange = (partition: TopicPartition, isLatest: boolean) => 
      Math.max(
        partition.earliestOffset,
        partition.latestOffset - (numberOfPages(topic) - page - Number(isLatest) + 1) * NUM_OF_EVENTS_PER_PAGE
    )
    
    if (isStreaming) {
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
    } else if (eventSource.current) {
      eventSource.current?.close()
      eventSource.current = null
    } else {
      const params = topic.partitions
      .map(partition => ({ 
        [`t0p${partition.id}e`]: offsetRange(partition, false),
        [`t0p${partition.id}l`]: offsetRange(partition, true)
      }))
      .reduce((acc: any, it: any) => ({ ...acc, ...it }), ({ t0: topicName } as any))
    
    setLoadingMessage('Loading events...')
    Axios.get<KafkaEvent[]>('/api/events', { params })
      .then(({ data }) => {
        const partitionsEvents: PartitionedEvents = {}
        data.forEach(event => {
          const partition = event.partition
          partitionsEvents[partition] = [event, ...(partitionsEvents[partition] || [])]  
        })
        setEvents(partitionsEvents)
      })
      .finally(() => {
        setLoadingMessage(null)
      })
    }
    
  // eslint-disable-next-line
  }, [topicName, topic, page, isStreaming])


  return (
    <>
      <Dimmer active={loadingMessage !== null} inverted>
        <Loader inverted>{ loadingMessage }</Loader>
      </Dimmer>

      <Pagination
          disabled={isStreaming}
          totalPages={numberOfPages(topic)} 
          activePage={page}
          onPageChange={(_, changedPage) => {
            setPage(Number(changedPage.activePage))
          }}
      />
      <Button icon basic onClick={() => setStreaming(!isStreaming)}>
        <Icon name={isStreaming ? 'pause' : 'play'} />
      </Button>

      <Divider/>
      
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