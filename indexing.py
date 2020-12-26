import random
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('--wq', help='operator <clean, index>')
parser.add_argument('--path', help='file path for indexing')
parser.add_argument('--k', help='file path for indexing')
args = parser.parse_args()

if args.wq == "clean_seq":
    with open("sequencer.txt", 'w', encoding='utf-8') as outfile:
        outfile.write("0")

if args.wq == "clean_hist":
    with open("histoutputs.csv.txt", 'w', encoding='utf-8') as outfile:
        outfile.write("0:1,1:2")

if args.wq == "index":
    f = open("security.csv","w+")
    f.write(args.k + ",0.00000001,0,0,0.5")#dist, epsilon, online, print sample output, sampling proportion (when oneline is 1)
    f.close()
    sequencer = 0
    with open("sequencer.txt", 'r', encoding='utf-8') as infile:
        for line in infile:
            sequencer = int(line.replace("\n",""))
            break

    # print(sequencer)
    # print(args.file.replace(".txt","") + "_write.txt")

    with open(args.path + ".gupa", 'w') as outfile:
        with open(args.path, 'r', encoding='utf-8') as infile:
            for line in infile:
                line1 = line.replace("\n","") + ';/;' + str(random.randint(sequencer, sequencer + int(args.k))) + '\n'
                outfile.write(line1)

    with open("sequencer.txt", 'w', encoding='utf-8') as outfile:
        outfile.write(str(sequencer + 1))
