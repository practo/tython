import optparse

def __get_comma_separated_args(option, opt, value, parser):
    setattr(parser.values, option.dest, value.split(','))

def get_options():
    parser = optparse.OptionParser()
    parser.add_option('-d', '--databases',
                      type='string',
                      action='callback',
                      callback=__get_comma_separated_args,
                      dest="dbs_to_be_updated",
                      default=[],
                      )
    parser.add_option('-m', '--no-monitoring',
                      dest="no_monitoring",
                      default=False,
                      action="store_true",
                      )
    parser.add_option('-n', '--num-ec2-instances',
                      dest="number_of_ec2_instances",
                      default=7,
                      type="int",
                      )
    parser.add_option('-t', '--ec2-instance-type',
                      dest="ec2_instance_type",
                      default="t2.large",
                      type="string",
                      )
    parser.add_option('-s', '--sentry-disabled',
                      dest="sentry_disabled",
                      default=False,
                      action="store_true",
                      )
    parser.add_option('-c', '--testing',
                      dest="testing",
                      default=False,
                      action="store_true",
                      )
    options, remainder = parser.parse_args()

    return options