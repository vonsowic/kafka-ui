import React from "react";
import { useCallback, useEffect, useState } from "react";
import Axios from 'axios'
import { Link, useParams } from "react-router-dom";
import { List } from "semantic-ui-react";


interface KafkaEvent {
  key: string
  value: string
  headers: Map<string, string>
}

function TopicView() {
    const { topicName } = useParams();
    const [events, setEvents] = useState([] as Array<KafkaEvent>)

    const addKafkaEvent = useCallback(
      (kafkaEvent: any) => {
        return setEvents(prevEvents => ([kafkaEvent, ...prevEvents]))
      },
      [events]
    ) 
    
    useEffect(() => {
      console.log(process.env)
      // const url = process.env.NODE_ENV === "development"
      //   ? `http://0.0.0.0:8080/api/topics/${topicName}/events?follow=true`
      //   : `/api/topics/${topicName}/events?follow=true`
      // const es = new EventSource(url) 
      // es.onmessage = (message) => addKafkaEvent(message.data)
      
      Axios.get<any[]>('/api/events', { params: { topic: topicName } })
        .then(({ data }) => {
          
          data.forEach(e => addKafkaEvent(e))
        })

      return () => {
        // es.close();
      };
    }, [])


    return (
      <div>
          <div>
            Number of fetched events: {events.length}
          </div>
          <List divided relaxed>
            {
              events.map((e, i) => 
                <List.Item key={i}>
                  <List.Content>
                    <List.Header as='a'>{JSON.stringify(e.key, null, 2)}</List.Header>
                    <List.Description as='a'>{JSON.stringify(e.value, null, 2)}</List.Description>
                  </List.Content>
                </List.Item>
              )
            }
            
          </List>
      </div>
    );
  }
  
  export default TopicView;