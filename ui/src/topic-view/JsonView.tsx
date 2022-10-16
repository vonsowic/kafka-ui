import ReactJson from 'react-json-view'
import { KafkaEventPart } from '../dto'


export interface JSONViewProps {
    event: KafkaEventPart
}
  
export function JSONView(props: JSONViewProps) {
    if (props.event.type === 'AVRO') {
        return <ReactJson 
            src={props.event.data}
            name={null}
        />
    } else {
        return <div> { props.event.data } </div>
    }
}