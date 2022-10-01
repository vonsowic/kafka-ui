import { assertExpressionStatement } from "@babel/types";
import { useEffect, useState } from "react";
import Axios from 'axios'
import { Link } from "react-router-dom";
import { Button, Input, List, Radio } from 'semantic-ui-react'

interface Topic {
  name: string
}

function TopicList() {
  const [includeTechnical, setIncludeTechnical] = useState(false)
  const [topicFilter, setTopicFilter] = useState('')
  const [topics, setTopics] = useState<Array<Topic>>([])
  useEffect(() => {
    Axios.get('/api/topics')
      .then(res => setTopics(res.data))
  }, [])

  return (
    <div>
      <Input icon='search' placeholder='Search...' onChange={e => setTopicFilter(e.target.value)} />
      <Radio
          label='Include technical'
          toggle 
          active={includeTechnical} 
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
    </div>
  );
}
  
export default TopicList;