#! /usr/bin/env python
import datetime
from mrjob.job import MRJob
from mrjob.step import MRStep

# Defining a class for the MapReduce job
class MRFatalJob(MRJob):
    
    # Configuring additional command line arguments
    def configure_args(self):
        super(MRFatalJob, self).configure_args()
        self.add_passthru_arg('--message', type=str, default='Power Good signal deactivated')
    
    # Mapper function: Processes each line of input
    def mapper(self, _, line):
        columns = line.split(' ')
        timeStamp = int(columns[1])
        date = datetime.datetime.fromtimestamp(timeStamp)
        msg = ' '.join(columns[8:])
        # Emit key-value pair if message contains specified search message and is of 'FATAL' level
        if self.options.message in msg and columns[8] == 'FATAL':
            yield None, date.strftime('%Y-%m-%d')
    
    # Combiner function: Combines intermediate data
    def combiner(self, _, dates):
        earliest_date = min(dates)
        yield None, earliest_date
    
    # Reducer function: Processes the combined data and produces the final output
    def reducer(self, _, dates):
        earliest_date = min(dates)
        yield "Earliest date with fatal kernel error containing 'Power Good signal deactivated':", earliest_date

# Execute the MapReduce job if this script is run directly
if __name__ == '__main__':
    MRFatalJob.run()
