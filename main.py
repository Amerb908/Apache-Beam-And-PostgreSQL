# This is to practice apache beam
import apache_beam as beam

p1 = beam.Pipeline()

PCollection = (
    p1
    | "Import Data" >> beam.io.ReadFromText("Data/voos_sample.csv")
    | "Split by comma" >> beam.Map(lambda record: record.split(","))
    #| beam.Map(lambda record: int(record[2]) * 10) this would go to the third column of the csv file, and multiply by 10
    #| beam.Map(print)
    | "Print Results" >> beam.io.WriteToText('Data/output.txt')
)

#p1 | 'Tuple' >> beam.Create([("Cassio", 32), ('Vics', 21)]) | beam.Map(print) #Adding new data onto the pipeline


p1.run()