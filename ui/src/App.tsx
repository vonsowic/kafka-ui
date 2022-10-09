import './App.css';
import TopicList from './topic-list';
import {
  BrowserRouter as Router,
  Routes,
  Route,
  Link
} from "react-router-dom";
import TopicView from './topic-view';
import SqlView from './sql';
import { Icon, Menu, Segment, Sidebar } from 'semantic-ui-react';

function App() {
  return (
    <div className='full-height'>
      <Router>

      <Sidebar.Pushable as={Segment} style={{ overflow: 'hidden' }} className='full-height'>
        <Sidebar
          as={Menu}
          width='thin'
          vertical
          inverted
          visible={true}>

          <Menu.Item as={Link} to='/'>
            <Icon name='home' />
            Topics
          </Menu.Item>
          <Menu.Item as={Link} to='/sql'>
            <Icon name='database' />
            SQL
          </Menu.Item>
        </Sidebar>
      
        <Sidebar.Pusher className='full-height'>
          <Segment basic>
                <Routes>
                  <Route path="/" element={<TopicList/>}/>
                  <Route path="/topics/:topicName" element={<TopicView/>}/>
                  <Route path="/sql" element={<SqlView/>}/>
                </Routes>
            </Segment>
        </Sidebar.Pusher>
      </Sidebar.Pushable>
      </Router>
  </div>
  )
}

export default App;
