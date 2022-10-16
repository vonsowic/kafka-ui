import { useEffect, useState } from "react";
import Axios from 'axios'
import { Link } from "react-router-dom";
import {  Dimmer, Input, List, Loader, Radio } from 'semantic-ui-react'
import { Topic } from "../dto";

function TopicList() {
  const [includeTechnical, setIncludeTechnical] = useState(false)
  const [topicFilter, setTopicFilter] = useState('')
  const [topics, setTopics] = useState<Topic[]>([])
  const [loadingMessage, setLoadingMessage] = useState<string | null>('Loading topic list...')

  useEffect(() => {
    Axios.get('/api/topics')
      .then(res => setTopics(res.data))
      .finally(() => setLoadingMessage(null))
  }, [])

  return (
    <>
      <Dimmer active={loadingMessage !== null} inverted>
        <Loader inverted>{ loadingMessage }</Loader>
      </Dimmer>
      <Input icon='search' placeholder='Search...' onChange={e => setTopicFilter(e.target.value)} />
      <Radio
          label='Include technical'
          toggle 
          onClick={() => setIncludeTechnical(!includeTechnical)}>
      </Radio>
      <List>
        {
          topics.filter(topic => topic.name.includes(topicFilter))
                .filter(topic => includeTechnical || !topic.name.startsWith('_'))
                .map((topic, i) => 
                  <List.Item key={i}> 
                    <Link to={`/topics/${topic.name}`}>{topic.name}</Link>
                  </List.Item>
                )
        }
        
      </List>
    </>
  );
}
  
export default TopicList;