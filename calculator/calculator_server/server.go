package main

import(
	"fmt"
	"log"
	"net"
	"math"
	"io"
	"context"

	"github.com/villegasl/go_grpc_course/calculator/calculatorpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/codes"
)

type server struct{}

// Unary API
func (*server) Sum(ctx context.Context, req *calculatorpb.SumRequest) (*calculatorpb.SumResponse, error) {
	fmt.Printf("Sum function was invoked with: %v\n",req)
	num1 := req.GetNum1()
	num2 := req.GetNum2()

	res := &calculatorpb.SumResponse {
		Result: num1 + num2,
	}
	return res, nil
}

// Server Streaming API
func (*server) DecompositeNumber(req *calculatorpb.DecompositeNumberRequest, stream calculatorpb.CalculatorService_DecompositeNumberServer) error {
	fmt.Printf("DecompositeNumber function was invoked with: %v\n", req)

	number := req.GetNumber()
	res := new(calculatorpb.DecompositeNumberResponse)

	divisor := int64(2)
	for number > 1 {
		if number % divisor == 0 {
			// this is a factor
			res.PrimeFactor = divisor
			stream.Send(res)
			// divide number by divisor so that we have the rest of the number left
			number = number / divisor 
		} else {
			divisor++
			fmt.Printf("Divisor has increased to %v\n", divisor)
		}
	}
	return nil
}

// Client Streaming API
func (*server) ComputeAverage(stream calculatorpb.CalculatorService_ComputeAverageServer) error {
	fmt.Printf("ComputeAverage function was invoked")

	average := float64(0)
	sum := int64(0)
	count := 0

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			// We've finished reading the client stream
			average = float64(sum) / float64(count)
			return stream.SendAndClose(&calculatorpb.ComputeAverageResponse {
				Average: average,
			})
		} else if err != nil {
			log.Fatalf("Error while reading client stream: %v",err)
			return err
		}
		sum += req.GetNumber()
		count++
	}
}

// Bidirectional Streaming API
func (*server) FindMaximum(stream calculatorpb.CalculatorService_FindMaximumServer) error {
	fmt.Printf("FindMaximun function was invoked\n")

	maxNum := int32(0)
	currentNum := int32(0)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			log.Fatalf("Error while reading stream request: %v", err)
			return err
		}
		currentNum = req.GetNumber()
		log.Printf("%v ", currentNum)
		if currentNum > maxNum {
			maxNum = currentNum
			sendErr := stream.Send(&calculatorpb.FindMaximumResponse{
				Max: maxNum,
			})
			if sendErr != nil {
				log.Fatalf("Error while sending stream response: %v", err)
				return sendErr
			}
		}
	}
}

func (*server) SquareRoot(ctx context.Context, req *calculatorpb.SquareRootRequest) (*calculatorpb.SquareRootResponse, error) {
	log.Printf("Received SquareRoot RPC\n")
	number := req.GetNumber()
	if number < 0 {
		return nil, status.Errorf(
			codes.InvalidArgument,
			fmt.Sprintf("Received a negative number: %v", number),
		)
	}
	return &calculatorpb.SquareRootResponse{
		NumberRoot: math.Sqrt(float64(number)),
	}, nil
}

func main() {
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	calculatorpb.RegisterCalculatorServiceServer(s, &server{})

	fmt.Printf("Ready to serve.\n")
	err = s.Serve(lis)
	if err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

/*
[Exercise] FindMaximum API
Now it is your turn to write code!

In this exercise, your goal is to implement a FindMaximum RPC Bi-Directional Streaming API in a CalculatorService:

The function takes a stream of Request message that has one integer, and returns a stream of Responses that represent the current maximum between all these integers
Remember to first implement the service definition in a .proto file, alongside the RPC messages
Implement the Server code first
Test the server code by implementing the Client
Example:

The client will send a stream of number (1,5,3,6,2,20) and the server will respond with a stream of (1,5,6,20)
*/