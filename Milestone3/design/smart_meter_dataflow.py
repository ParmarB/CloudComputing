import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import argparse

class ProcessMeasurements(beam.DoFn):
    def process(self, element):
        # 1. Deserialize the JSON string
        data = json.loads(element)
        
        # 2. Filter: Eliminate records with missing measurements (containing None)
        if data.get('pressure') is None or data.get('temperature') is None:
            return

        # 3. Convert units
        # P(psi) = P(kPa) / 6.895
        data['pressure_psi'] = data['pressure'] / 6.895
        # T(F) = T(C) * 1.8 + 32
        data['temperature_fahrenheit'] = (data['temperature'] * 1.8) + 32
        
        # 4. Serialize back to JSON string for Pub/Sub
        yield json.dumps(data).encode('utf-8')

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', required=True, help='Input Pub/Sub topic')
    parser.add_argument('--output_topic', required=True, help='Output Pub/Sub topic')
    args, pipeline_args = parser.parse_known_args()

    options = PipelineOptions(pipeline_args, streaming=True)
    
    with beam.Pipeline(options=options) as p:
        (
            p 
            | "Read From PubSub" >> beam.io.ReadFromPubSub(topic=args.input_topic)
            | "Decode & Process" >> beam.ParDo(ProcessMeasurements())
            | "Write To PubSub" >> beam.io.WriteToPubSub(topic=args.output_topic)
        )

if __name__ == '__main__':
    run()