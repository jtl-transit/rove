import logging
logger = logging.getLogger("shapeGenLogger")

# main for testing
def __main__():
    logger.info(f'Starting shape generation...')
    print(f'in main method of shape gen')

def generate_shapes(feed):
    print(f'running shape gen')
    logger.info('logging to shape gen logger')

# shape_gen_test()
if __name__ == "__main__":
    __main__()