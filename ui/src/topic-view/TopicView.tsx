import React from "react";
import { useCallback, useEffect, useState } from "react";
import Axios from 'axios'
import { Link, useParams } from "react-router-dom";
import { Dimmer, Icon, List, Loader, Menu } from "semantic-ui-react";

interface KafkaEventPart {
  data: any
  type: 'STRING' | 'AVRO' | 'NIL'
}

interface KafkaEvent {
  partition: number
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
const NUM_OF_EVENTS_PER_PAGE = 5


function getTopic(name: string): Promise<Topic> {
  return Axios.get(`/api/topics/${name}`)
    .then(({ data }) => data)
}

function numberOfPages(topic: Topic | undefined): number {
  if (!topic) {
    return 0
  }

  const numbersOfRecors = topic.partitions.map(partition => partition.latestOffset - partition.earliestOffset)
  return Math.ceil(Math.max(...numbersOfRecors) / NUM_OF_EVENTS_PER_PAGE) + 1
}


function TopicView() {
    const { topicName } = useParams();
    if (topicName == undefined) {
      throw Error('topic param must be defined')
    }

    const [isLoading, setLoading] = useState(true)
    const [page, setPage] = useState(0)
    const [events, setEvents] = useState([] as Array<KafkaEvent>)
    const [topic, setTopic] = useState(undefined as any as Topic)
    // const addKafkaEvent = useCallback(
    //   (kafkaEvent: any) => {
    //     return setEvents(prevEvents => ([kafkaEvent, ...prevEvents]))
    //   },
    //   [events]
    // ) 
    
    useEffect(() => {
      getTopic(topicName)
        .then(topic => {
          setPage(numberOfPages(topic) - 1)
          setTopic(topic)
        })
    }, [])

    useEffect(() => {
      if (!topic) {
        return
      }

      // const url = process.env.NODE_ENV === "development"
      //   ? `http://0.0.0.0:8080/api/topics/${topicName}/events?follow=true`
      //   : `/api/topics/${topicName}/events?follow=true`
      // const es = new EventSource(url) 
      // es.onmessage = (message) => addKafkaEvent(message.data)


      const offsetRange = (partition: TopicPartition, isLatest: boolean) => 
        Math.max(
          partition.earliestOffset,
          partition.latestOffset - (numberOfPages(topic) - page - Number(isLatest)) * NUM_OF_EVENTS_PER_PAGE
      )
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
    }, [page])


    return (
      <div>
        <Dimmer active={isLoading} inverted>
          <Loader inverted>Loading</Loader>
        </Dimmer>
        <Menu pagination>
          {
            Array.from(Array(numberOfPages(topic)).keys())
              // .slice(selectPageButtonOffset, selectPageButtonOffset + selectPageButtonsNum)
              .map(p => (
                <Menu.Item 
                  key={p}
                  onClick={() => setPage(p)}
                  active={p === page} as='a'>
                    { p }
                </Menu.Item>
              ))
          }
        </Menu>
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