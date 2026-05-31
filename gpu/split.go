package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"mvdan.cc/sh/v3/syntax"
)

type chunk struct {
	startLine int
	endLine   int
	name      string
	isFunc    bool
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <input_file>\n", os.Args[0])
		os.Exit(1)
	}
	inputFile := os.Args[1]
	outputDir := inputFile + ".d"

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output directory: %v\n", err)
		os.Exit(1)
	}

	content, err := ioutil.ReadFile(inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input file: %v\n", err)
		os.Exit(1)
	}
	scriptContent := string(content)
	lines := strings.Split(scriptContent, "\n")

	parser := syntax.NewParser()
	f, err := parser.Parse(strings.NewReader(scriptContent), "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing script: %v\n", err)
		os.Exit(1)
	}

	var chunks []chunk
	syntax.Walk(f, func(node syntax.Node) bool {
		if node == nil {
			return false
		}

		switch x := node.(type) {
		case *syntax.FuncDecl:
			chunks = append(chunks, chunk{
				startLine: int(x.Pos().Line()),
				endLine:   int(x.End().Line()),
				name:      x.Name.Value,
				isFunc:    true,
			})
			return false // Don't descend into function body
		}
		return true
	})

	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].startLine < chunks[j].startLine
	})

	var fileIndex int
	lastLine := 0

	writeChunk := func(start, end int, name string) {
		if start > end || start <= 0 || end <= 0 {
			return
		}
		fileName := fmt.Sprintf("%03d_%s.sh", fileIndex, name)
		filePath := filepath.Join(outputDir, fileName)
		fileIndex++

		fmt.Printf("Extracting lines %d to %d to %s\n", start, end, filePath)
		outFile, err := os.Create(filePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
			return
		}
		defer outFile.Close()

		writer := bufio.NewWriter(outFile)
		for i := start - 1; i < end && i < len(lines); i++ {
			fmt.Fprintln(writer, lines[i])
		}
		writer.Flush()
	}

	// Header
	if len(chunks) > 0 && chunks[0].startLine > 1 {
		writeChunk(1, chunks[0].startLine-1, "header")
		lastLine = chunks[0].startLine - 1
	} else if len(chunks) == 0 {
		writeChunk(1, len(lines), "header")
		lastLine = len(lines)
	}

	for _, c := range chunks {
		// Interim
		if c.startLine > lastLine+1 {
			writeChunk(lastLine+1, c.startLine-1, "interim")
		}

		// Function
		writeChunk(c.startLine, c.endLine, c.name)
		lastLine = c.endLine
	}

	// Footer (after the last function)
	if lastLine < len(lines) {
		finalEndLine := len(lines)
		if len(lines) > 0 && lines[len(lines)-1] == "" {
			finalEndLine--
		}
		if lastLine < finalEndLine {
		    writeChunk(lastLine+1, finalEndLine, "footer")
        }
	}

	fmt.Println("Splitting complete.")
}
