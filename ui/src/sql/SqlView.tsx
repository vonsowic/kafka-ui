import { useCallback, useState } from "react";
import Axios from 'axios'
import { Button, Dimmer, Divider, Form, Loader, Message, Pagination, Segment, Table, TextArea } from "semantic-ui-react";


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
    const numberOfRowsPerPage = 50
    const numberOfPages = Math.ceil(props.rows.length / numberOfRowsPerPage)

    const [page, setPage] = useState(1)

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
            props.rows.slice((page - 1) * numberOfRowsPerPage, page * numberOfRowsPerPage).map((row, i) => 
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
              <Pagination 
                totalPages={numberOfPages} 
                activePage={page}
                onPageChange={(_, changedPage) => {
                  setPage(Number(changedPage.activePage))
                }}
              />
            </Table.HeaderCell>
          </Table.Row>
        </Table.Footer>
      </Table>
    )
  }

  function Readme() {
    return (
      <div>
        <Message warning>
          Experimental feature
        </Message>
        <Segment>
        Read Kafka as it was a SQL database. By executing SQL statement, if the topic has a schema, it is automatically converted to database schema
        </Segment>
      </div>
    )
  }

  export default SqlView;