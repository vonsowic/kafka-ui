import { useCallback, useEffect, useRef, useState } from "react";
import Axios from 'axios'
import { useParams } from "react-router-dom";
import { Button, Dimmer, Icon, List, Loader, Pagination } from "semantic-ui-react";

interface KafkaEventPart {
  data: any
  type: 'STRING' | 'AVRO' | 'NIL'
}

interface KafkaEvent {
  topic: string
  partition: number
  offset: number
  key: KafkaEventPart
  value: KafkaEventPart
  headers: Map<string, string>
}

interface Topic {
  name: string,
  partitions: TopicPartition[]
}

interface TopicPartition {
  id: number,
  earliestOffset: number,
  latestOffset: number
}

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
    const [isLoading, setLoading] = useState(true)
    const [page, setPage] = useState(0)
    const [events, setEvents] = useState([] as Array<KafkaEvent>)
    const [topic, setTopic] = useState(undefined as any as Topic)
    const addKafkaEvent = useCallback(
      (kafkaEvent: any) => {
        return setEvents(prevEvents => ([JSON.parse(kafkaEvent), ...prevEvents]))
      },
      // previous version: [events]
      []
    ) 

    useEffect(() => {
      if (!topic) {
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
        const params = { t0: topicName } as any
        events.forEach(event => {
          const partitionKey = `t0p${event.partition}e`
          const offset = event.offset + 1 
          if (offset > (params[partitionKey] || -1)) {
            params[partitionKey] = offset
          }
        })

        const paramStr =
           Object.entries(params)
            .map(([key, value]) => `${key}=${value}`)
            .join('&')

        const url = process.env.NODE_ENV === "development"
          ? `http://0.0.0.0:8080/api/events?${paramStr}`
          : `/api/events?${paramStr}`

        eventSource.current = new EventSource(url) 
        eventSource.current.onmessage = (message) => addKafkaEvent(message.data)
      } else {
        if (eventSource) {
          eventSource.current?.close()
          eventSource.current = null
        }

        const params = topic.partitions
          .map(partition => ({ 
            [`t0p${partition.id}e`]: offsetRange(partition, false),
            [`t0p${partition.id}l`]: offsetRange(partition, true)
          }))
          .reduce((acc: any, it: any) => ({ ...acc, ...it }), ({ t0: topicName } as any))
        setLoading(true)
        Axios.get<any[]>('/api/events', { params })
          .then(({ data }) => {
            setEvents(data)
          })
          .finally(() => {
            setLoading(false)
          })
      }
    }, [topicName, topic, page, isStreaming])


    return (
      <div>
        <Dimmer active={isLoading} inverted>
          <Loader inverted>Loading</Loader>
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
        <List divided relaxed>
          {
            events.map((e, i) => 
              <List.Item key={i}>
                <List.Content>
                  <List.Header as='a'>{JSON.stringify(e.key.data, null, 2)}</List.Header>
                  <List.Description as='a'>{JSON.stringify(e.value.data, null, 2)}</List.Description>
                </List.Content>

              </List.Item>
            )
          }
        </List>
      </div>
    );
  }
  
  export default TopicView;