# Distributed Photo Collage
Final project for the Distributed Computing course. Given a folder with several images, creates a single collage made of all of those images.

## Information about the project
The two main components are the broker and worker programs. The broker is launched locally, at the defined port in its init parameters. Then, it waits for workers to connect to it so that it can distribute images to resize or merge. All communication is done via UDP, with JSON messages to coordinate the process. The final output is a single merged image made of all the original images.

The full information about the project can be found in the report -> [report.pdf](./report.pdf) 

## Usage
First, the broker needs to be launched with two arguments: the path to the folder containing the images, and the height of the final collage

```sh
python3 broker.py image_folder desired_height
```

Then, several workers can be launched, with the argument being the broker's address

```sh
python3 worker.py broker_address
```
