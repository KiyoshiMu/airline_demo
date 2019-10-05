from airline_demo.trainer import preprocess

if __name__ == "__main__":
    ARGV = [
        '--train-data-file=data/train.csv',
        '--test-data-file=data/val.csv',
        '--root-train-data-out=train',
        '--root-test-data-out=test',
        '--working-dir=tmp'
    ]
    preprocess.main(ARGV)