import { useCallback, useState } from "react";
import Axios from 'axios'
import { Button, Dimmer, Divider, Form, Icon, Loader, Menu, Message, Table, TextArea } from "semantic-ui-react";


type SqlResponse = object[]

function SqlView() {
  const [columnNames, setColumnNames] = useState([] as string[])
  const [rows, setRows] = useState(null as unknown as any[][])
  const [isLoading, setLoading] = useState(false)
  const [sql, setSql] = useState('')

  const [errorTitle, setErrorTitle] = useState('')
  const [errorMessage, setErrorMessage] = useState('')

  const sendRequest = useCallback(() => {
    setLoading(true)
    Axios.post<SqlResponse>('/api/sql', { sql: sql })
      .then(({ data }) => {
        setErrorTitle('')
        setErrorMessage('')
        if (!data) {
          return
        }

        const col = Object.keys(data[0])
        setColumnNames(col)
        setRows(data.map(row => Object.values(row)))
      })
      .catch(err => {
        setErrorTitle(err.message)
        setErrorMessage(err.response.data.message)
      })
      .finally(() => {
        setLoading(false)
      })
  }, [sql]);


  return (
    <div>
      <Form>
        <TextArea 
          rows={1}
          placeholder='Type you SQL here...'
          value={sql} 
          onChange={event => {
            setSql(event.target.value)
          }}
          />
      </Form>
      <Button primary onClick={sendRequest}>Execute</Button>


      {
        errorMessage === '' ? <div/> 
          :  <Message
              error
              header={errorTitle}
              content={errorMessage}
            /> 
      }
     

      <Divider></Divider>

      <Dimmer active={isLoading}>
        <Loader content='Loading' />
      </Dimmer>

      {
        rows !== null 
          ? <SqlRowsView 
              columnNames={columnNames}
              rows={rows}
          />
          : <Readme></Readme>
      }
    </div>
  );
}


  interface SqlRowsViewProps {
    columnNames: string[]
    rows: any[][]
  }

  function SqlRowsView(props: SqlRowsViewProps) {
    const selectPageButtonsNum = 20
    const numberOfRowsPerPage = 50
    const numberOfPages = Math.ceil(props.rows.length / numberOfRowsPerPage)

    const [page, setPage] = useState(0)
    const [selectPageButtonOffset, setSelectPageButtonOffset] = useState(0)

    return (
      <Table celled>
        <Table.Header>
          <Table.Row>
            {
              props.columnNames.map(column =>
                  <Table.HeaderCell key={column}>
                    { column }
                  </Table.HeaderCell>
                )
            }
          </Table.Row>
        </Table.Header>

        <Table.Body>
          {
            props.rows.slice(page * numberOfRowsPerPage, (page + 1) * numberOfRowsPerPage).map((row, i) => 
              <Table.Row key={page * numberOfRowsPerPage + i}>
                { 
                  row.map((cell, cellIndex) => 
                    <Table.Cell key={page * numberOfRowsPerPage + i + "-" + cellIndex}>
                      {cell === null ? <span style={{color: 'red'}}>null</span> : cell}
                    </Table.Cell>
                  )
              }
            </Table.Row>
          )
          }
        </Table.Body>

        <Table.Footer>
          <Table.Row>
            <Table.HeaderCell colSpan={props.columnNames.length + 2}>
              <Menu pagination>
                <Menu.Item as='a' icon
                    onClick={() => {
                      const newOffset = selectPageButtonOffset - numberOfRowsPerPage
                      if (newOffset > 0) {
                        setSelectPageButtonOffset(newOffset)
                      } else {
                        setSelectPageButtonOffset(0)
                      }
                    }}>
                  <Icon name='chevron left' />
                </Menu.Item>

                <Menu.Item 
                    as='a'
                    icon 
                    onClick={() => {
                      const newOffset = selectPageButtonOffset + numberOfRowsPerPage
                      if (newOffset < numberOfPages) {
                        setSelectPageButtonOffset(newOffset)
                      } 
                    }}>
                  <Icon name='chevron right' />
                </Menu.Item>

                {
                  Array.from(Array(numberOfPages).keys())
                    .slice(selectPageButtonOffset, selectPageButtonOffset + selectPageButtonsNum)
                    .map(p => (
                      <Menu.Item 
                        key={p}
                        onClick={() => setPage(p)}
                        active={p === page} as='a'>
                          { p + 1 }
                      </Menu.Item>
                    ))
                }
              </Menu>
            </Table.HeaderCell>
          </Table.Row>
        </Table.Footer>
      </Table>
    )
  }

  function Readme() {
    return (
      <div>
        Read Kafka as it was a SQL database. By executing SQL statement, if the topic has a schema, it is automatically converted to database schema
      </div>
    )
  }

  export default SqlView;