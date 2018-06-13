package brimtext_test

import (
	"fmt"
	"testing"

	"github.com/gholt/brimtext"
)

func TestAlignUnicode(t *testing.T) {
	opts := &brimtext.AlignOptions{}
	opts.RowFirstUD = ">>>"
	opts.RowLastUD = "<<<"
	opts.RowSecondUD = "||"
	opts.RowUD = "||"
	opts.LeaveTrailingWhitespace = true
	opts.Alignments = []brimtext.Alignment{brimtext.Left, brimtext.Right, brimtext.Right, brimtext.Left}
	opts.Widths = []int{0, 5, 10, 5}
	out := brimtext.Align([][]string{
		[]string{"", "one", "two", "three"},
		[]string{"a", "one a b \u0041 \u00c0 \uff21 \U0001d400 d efg", "two abc d ef \u0041 \u00c0 \uff21 \U0001d400", "three a \u0041 \u00c0 \uff21 \U0001d400 bcd efg hij"},
	}, opts)
	exp := `>>> ||  one||       two||three<<<
>>>a||one a|| two abc d||three<<<
>>> ||b A À||ef A À Ａ 𝐀||a A À<<<
>>> ||Ａ 𝐀 d||          ||Ａ 𝐀  <<<
>>> ||  efg||          ||bcd  <<<
>>> ||     ||          ||efg  <<<
>>> ||     ||          ||hij  <<<
`
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}
}

func TestAlign(t *testing.T) {
	out := brimtext.Align([][]string{
		[]string{"", "one", "two", "three"},
		[]string{"a", "one a", "two a", "three a"},
	}, nil)
	exp := `  one   two   three
a one a two a three a
`
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}
	opts := &brimtext.AlignOptions{}
	opts.RowFirstUD = ">>>"
	out = brimtext.Align([][]string{
		[]string{"", "one", "two", "three"},
		[]string{"a", "one a", "two a", "three a"},
	}, opts)
	exp = `>>> one  two  three
>>>aone atwo athree a
`
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}
	opts.RowLastUD = "<<<"
	out = brimtext.Align([][]string{
		[]string{"", "one", "two", "three"},
		[]string{"a", "one a", "two a", "three a"},
	}, opts)
	exp = `>>> one  two  three<<<
>>>aone atwo athree a<<<
`
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}
	opts.RowSecondUD = "||"
	opts.RowUD = "||"
	out = brimtext.Align([][]string{
		[]string{"", "one", "two", "three"},
		[]string{"a", "one a", "two a", "three a"},
	}, opts)
	exp = `>>> ||one  ||two  ||three<<<
>>>a||one a||two a||three a<<<
`
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}
	opts.Alignments = []brimtext.Alignment{brimtext.Left, brimtext.Right, brimtext.Center}
	out = brimtext.Align([][]string{
		[]string{"", "one", "two", "three"},
		[]string{"a", "one a", "two a", "three a"},
	}, opts)
	exp = `>>> ||  one|| two ||three<<<
>>>a||one a||two a||three a<<<
`
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}
	opts.Alignments = []brimtext.Alignment{brimtext.Left, brimtext.Right, brimtext.Center}
	opts.LeaveTrailingWhitespace = true
	out = brimtext.Align([][]string{
		[]string{"", "one", "two", "three"},
		[]string{"a", "one a", "two a", "three a"},
	}, opts)
	exp = `>>> ||  one|| two ||three  <<<
>>>a||one a||two a||three a<<<
`
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}
	out = brimtext.Align([][]string{
		[]string{"", "one", "two", "three"},
		nil,
		[]string{"a", "one a", "two a", "three a"},
	}, opts)
	exp = `>>> ||  one|| two ||three  <<<

>>>a||one a||two a||three a<<<
`
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}
	out = brimtext.Align(nil, opts)
	exp = ``
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}

	opts = brimtext.NewDefaultAlignOptions()
	out = brimtext.Align([][]string{
		[]string{"", "Bob", "Sue", "John"},
		[]string{"Hometown", "San Antonio", "Austin", "New York"},
		[]string{"Mother", "Bessie", "Mary", "Sarah"},
		[]string{"Father", "Rick", "Dan", "Mike"},
	}, opts)
	exp = `         Bob         Sue    John
Hometown San Antonio Austin New York
Mother   Bessie      Mary   Sarah
Father   Rick        Dan    Mike
`
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}

	opts = brimtext.NewSimpleAlignOptions()
	out = brimtext.Align([][]string{
		[]string{"", "Bob", "Sue", "John"},
		nil,
		[]string{"Hometown", "San Antonio", "Austin", "New York"},
		[]string{"Mother", "Bessie", "Mary", "Sarah"},
		[]string{"Father", "Rick", "Dan", "Mike"},
	}, opts)
	exp = `+----------+-------------+--------+----------+
|          | Bob         | Sue    | John     |
+----------+-------------+--------+----------+
| Hometown | San Antonio | Austin | New York |
| Mother   | Bessie      | Mary   | Sarah    |
| Father   | Rick        | Dan    | Mike     |
+----------+-------------+--------+----------+
`
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}

	opts = brimtext.NewBoxedAlignOptions()
	out = brimtext.Align([][]string{
		[]string{"", "Bob", "Sue", "John"},
		[]string{"Hometown", "San Antonio", "Austin", "New York"},
		[]string{"Mother", "Bessie", "Mary", "Sarah"},
		[]string{"Father", "Rick", "Dan", "Mike"},
	}, opts)
	exp = `+==========+=============+========+==========+
|          | Bob         | Sue    | John     |
+==========+=============+========+==========+
| Hometown | San Antonio | Austin | New York |
+----------+-------------+--------+----------+
| Mother   | Bessie      | Mary   | Sarah    |
+----------+-------------+--------+----------+
| Father   | Rick        | Dan    | Mike     |
+==========+=============+========+==========+
`
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}

	opts = brimtext.NewUnicodeBoxedAlignOptions()
	out = brimtext.Align([][]string{
		[]string{"", "Bob", "Sue", "John"},
		[]string{"Hometown", "San Antonio", "Austin", "New York"},
		[]string{"Mother", "Bessie", "Mary", "Sarah"},
		[]string{"Father", "Rick", "Dan", "Mike"},
	}, opts)
	exp = `╔══════════╦═════════════╤════════╤══════════╗
║          ║ Bob         │ Sue    │ John     ║
╠══════════╬═════════════╪════════╪══════════╣
║ Hometown ║ San Antonio │ Austin │ New York ║
╟──────────╫─────────────┼────────┼──────────╢
║ Mother   ║ Bessie      │ Mary   │ Sarah    ║
╟──────────╫─────────────┼────────┼──────────╢
║ Father   ║ Rick        │ Dan    │ Mike     ║
╚══════════╩═════════════╧════════╧══════════╝
`
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}

	opts = brimtext.NewBoxedAlignOptions()
	opts.Widths = []int{0, 10}
	opts.Alignments = []brimtext.Alignment{brimtext.Left, brimtext.Right}
	out = brimtext.Align([][]string{
		[]string{"", "one", "two", "three"},
		nil,
		[]string{"a", "one a and more text that should be wrapped", "two a", "three a"},
		[]string{"b", "one b", "two b", "three b"},
		nil,
		[]string{"c", "one c", "two c", "three c"},
	}, opts)
	exp = `+===+===========+=======+=========+
|   |       one | two   | three   |
+===+===========+=======+=========+
| a | one a and | two a | three a |
|   | more text |       |         |
|   |      that |       |         |
|   | should be |       |         |
|   |   wrapped |       |         |
+---+-----------+-------+---------+
| b |     one b | two b | three b |
+---+-----------+-------+---------+
| c |     one c | two c | three c |
+===+===========+=======+=========+
`
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}

	opts = brimtext.NewDefaultAlignOptions()
	opts.Widths = []int{0, 10}
	opts.Alignments = []brimtext.Alignment{brimtext.Left, brimtext.Right}
	out = brimtext.Align([][]string{
		[]string{"", "one", "two", "three"},
		nil,
		[]string{"a", "one a and more text that should be wrapped", "two a", "three a"},
		[]string{"b", "one b", "two b", "three b"},
		nil,
		[]string{"c", "one c", "two c", "three c"},
	}, opts)
	exp = `        one two   three

a one a and two a three a
  more text       
       that       
  should be       
    wrapped       
b     one b two b three b

c     one c two c three c
`
	if out != exp {
		t.Errorf("%#v != %#v", out, exp)
	}
}

func ExampleAlign_default() {
	fmt.Println(brimtext.Align([][]string{
		{"", "Bob", "Sue", "John"},
		{"Hometown", "San Antonio", "Austin", "New York"},
		{"Mother", "Bessie", "Mary", "Sarah"},
		{"Father", "Rick", "Dan", "Mike"},
	}, nil))
	// Output:
	//          Bob         Sue    John
	// Hometown San Antonio Austin New York
	// Mother   Bessie      Mary   Sarah
	// Father   Rick        Dan    Mike
}

func ExampleAlign_simple() {
	fmt.Println(brimtext.Align([][]string{
		{"", "Bob", "Sue", "John"},
		nil,
		{"Hometown", "San Antonio", "Austin", "New York"},
		{"Mother", "Bessie", "Mary", "Sarah"},
		{"Father", "Rick", "Dan", "Mike"},
	}, brimtext.NewSimpleAlignOptions()))
	// Output:
	// +----------+-------------+--------+----------+
	// |          | Bob         | Sue    | John     |
	// +----------+-------------+--------+----------+
	// | Hometown | San Antonio | Austin | New York |
	// | Mother   | Bessie      | Mary   | Sarah    |
	// | Father   | Rick        | Dan    | Mike     |
	// +----------+-------------+--------+----------+
}

func ExampleAlign_unicodeBoxed() {
	data := [][]int{
		{8, 20, 11},
		{5, 11, 10},
		{3, 9, 1},
		{1200000, 2400000, 1700000},
	}
	table := [][]string{{"", "Bob", "Sue", "John"}}
	for rowNum, values := range data {
		row := []string{""}
		prefix := ""
		switch rowNum {
		case 0:
			row[0] = "Shot Attempts"
		case 1:
			row[0] = "Shots Made"
		case 2:
			row[0] = "Shots Missed"
		case 3:
			row[0] = "Salary"
			prefix = "$"
		}
		for _, v := range values {
			row = append(row, prefix+brimtext.ThousandsSep(int64(v), ","))
		}
		table = append(table, row)
	}
	opts := brimtext.NewUnicodeBoxedAlignOptions()
	opts.Alignments = []brimtext.Alignment{
		brimtext.Right,
		brimtext.Right,
		brimtext.Right,
		brimtext.Right,
	}
	fmt.Println(brimtext.Align(table, opts))
	// Output:
	// ╔═══════════════╦════════════╤════════════╤════════════╗
	// ║               ║        Bob │        Sue │       John ║
	// ╠═══════════════╬════════════╪════════════╪════════════╣
	// ║ Shot Attempts ║          8 │         20 │         11 ║
	// ╟───────────────╫────────────┼────────────┼────────────╢
	// ║    Shots Made ║          5 │         11 │         10 ║
	// ╟───────────────╫────────────┼────────────┼────────────╢
	// ║  Shots Missed ║          3 │          9 │          1 ║
	// ╟───────────────╫────────────┼────────────┼────────────╢
	// ║        Salary ║ $1,200,000 │ $2,400,000 │ $1,700,000 ║
	// ╚═══════════════╩════════════╧════════════╧════════════╝
}

func ExampleAlign_unicodeCustom() {
	opts := brimtext.NewUnicodeBoxedAlignOptions()
	opts.FirstFirstDLR = opts.FirstDLR
	opts.RowSecondUD = opts.RowUD
	opts.NilFirstUDLR = opts.NilUDLR
	opts.FirstNilFirstUDR = opts.NilFirstUDR
	opts.FirstNilLR = opts.NilLR
	opts.FirstNilFirstUDLR = opts.NilFirstUDLR
	opts.FirstNilUDLR = opts.NilUDLR
	opts.FirstNilLastUDL = opts.NilLastUDL
	opts.LastFirstULR = opts.LastULR
	opts.NilBetweenEveryRow = false
	opts.Alignments = []brimtext.Alignment{
		brimtext.Left,
		brimtext.Right,
		brimtext.Right,
	}
	fmt.Println(brimtext.Align([][]string{
		{"Name", "Points", "Assists"},
		nil,
		{"Bob", "10", "1"},
		{"Sue", "7", "5"},
		{"John", "2", "1"},
		nil,
		{"Shooting Stars", "19", "7"},
	}, opts))
	// Output:
	// ╔════════════════╤════════╤═════════╗
	// ║ Name           │ Points │ Assists ║
	// ╟────────────────┼────────┼─────────╢
	// ║ Bob            │     10 │       1 ║
	// ║ Sue            │      7 │       5 ║
	// ║ John           │      2 │       1 ║
	// ╟────────────────┼────────┼─────────╢
	// ║ Shooting Stars │     19 │       7 ║
	// ╚════════════════╧════════╧═════════╝
}
