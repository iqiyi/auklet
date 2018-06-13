// Package brimtext contains tools for working with text. Probably the most
// complex of these tools is Align, which allows for formatting "pretty
// tables".
package brimtext

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

// OrdinalSuffix returns "st", "nd", "rd", etc. for the number given (1st, 2nd,
// 3rd, etc.).
func OrdinalSuffix(number int) string {
	if (number/10)%10 == 1 || number%10 > 3 {
		return "th"
	} else if number%10 == 1 {
		return "st"
	} else if number%10 == 2 {
		return "nd"
	} else if number%10 == 3 {
		return "rd"
	}
	return "th"
}

// ThousandsSep returns the number formatted using the separator at each
// thousands position, such as ThousandsSep(1234567, ",") giving 1,234,567.
func ThousandsSep(v int64, sep string) string {
	s := strconv.FormatInt(v, 10)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

// ThousandsSepU returns the number formatted using the separator at each
// thousands position, such as ThousandsSepU(1234567, ",") giving 1,234,567.
func ThousandsSepU(v uint64, sep string) string {
	s := strconv.FormatUint(v, 10)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func humanSize(v float64, u float64, s []string) string {
	n := v
	i := 0
	for ; i < len(s); i++ {
		if math.Ceil(n) < 1000 {
			break
		}
		n = n / u
	}
	if i >= len(s) {
		return fmt.Sprintf("%.0f%s", n*u, s[len(s)-1])
	}
	if i == 0 {
		return fmt.Sprintf("%.4g", n)
	}
	if n < 1 {
		return fmt.Sprintf("%.2g%s", n, s[i])
	}
	return fmt.Sprintf("%.3g%s", n, s[i])
}

// HumanSize1000 returns a more readable size format, such as
// HumanSize1000(1234567) giving "1.23m".
// These are 1,000 unit based: 1k = 1000, 1m = 1000000, etc.
func HumanSize1000(v float64) string {
	return humanSize(v, 1000, []string{"", "k", "m", "g", "t", "p", "e", "z", "y"})
}

// HumanSize1024 returns a more readable size format, such as
// HumanSize1024(1234567) giving "1.18M".
// These are 1,024 unit based: 1K = 1024, 1M = 1048576, etc.
func HumanSize1024(v float64) string {
	return humanSize(v, 1024, []string{"", "K", "M", "G", "T", "P", "E", "Z", "Y"})
}

// Sentence converts the value into a sentence, uppercasing the first character
// and ensuring the string ends with a period. Useful to output better looking
// error.Error() messages, which are all lower case with no trailing period by
// convention.
func Sentence(value string) string {
	if value != "" {
		if value[len(value)-1] != '.' {
			value = strings.ToUpper(value[:1]) + value[1:] + "."
		} else {
			value = strings.ToUpper(value[:1]) + value[1:]
		}
	}
	return value
}

// StringSliceToLowerSort provides a sort.Interface that will sort a []string
// by their strings.ToLower values. This isn't exactly a case insensitive sort
// due to Unicode situations, but is usually good enough.
type StringSliceToLowerSort []string

func (s StringSliceToLowerSort) Len() int {
	return len(s)
}

func (s StringSliceToLowerSort) Swap(x int, y int) {
	s[x], s[y] = s[y], s[x]
}

func (s StringSliceToLowerSort) Less(x int, y int) bool {
	return strings.ToLower(s[x]) < strings.ToLower(s[y])
}

// Wrap wraps text for more readable output.
//
// The width can be a positive int for a specific width, 0 for the default
// width (attempted to get from terminal, 79 otherwise), or a negative number
// for a width relative to the default.
//
// The indent1 is the prefix for the first line.
//
// The indent2 is the prefix for any second or subsequent lines.
func Wrap(text string, width int, indent1 string, indent2 string) string {
	if width < 1 {
		width = GetTTYWidth() - 1 + width
	}
	bs := []byte(text)
	bs = wrap(bs, width, []byte(indent1), []byte(indent2))
	return string(bytes.Trim(bs, "\n"))
}

func wrap(text []byte, width int, indent1 []byte, indent2 []byte) []byte {
	if utf8.RuneCount(text) == 0 {
		return text
	}
	text = bytes.Replace(text, []byte{'\r', '\n'}, []byte{'\n'}, -1)
	var out bytes.Buffer
	for _, par := range bytes.Split([]byte(text), []byte{'\n', '\n'}) {
		par = bytes.Replace(par, []byte{'\n'}, []byte{' '}, -1)
		lineLen := 0
		start := true
		for _, word := range bytes.Split(par, []byte{' '}) {
			wordLen := utf8.RuneCount(word)
			if wordLen == 0 {
				continue
			}
			scan := word
			for len(scan) > 1 {
				i := bytes.IndexByte(scan, '\x1b')
				if i == -1 {
					break
				}
				j := bytes.IndexByte(scan[i+1:], 'm')
				if j == -1 {
					i++
				} else {
					j += 2
					wordLen -= j
					scan = scan[i+j:]
				}
			}
			if start {
				out.Write(indent1)
				lineLen += utf8.RuneCount(indent1)
				out.Write(word)
				lineLen += wordLen
				start = false
			} else if lineLen+1+wordLen > width {
				out.WriteByte('\n')
				out.Write(indent2)
				out.Write(word)
				lineLen = utf8.RuneCount(indent2) + wordLen
			} else {
				out.WriteByte(' ')
				out.Write(word)
				lineLen += 1 + wordLen
			}
		}
		out.WriteByte('\n')
		out.WriteByte('\n')
	}
	return out.Bytes()
}

// AllEqual returns true if all the values are equal strings; no strings,
// AllEqual() or AllEqual([]string{}...), are considered AllEqual.
func AllEqual(values ...string) bool {
	if len(values) < 2 {
		return true
	}
	compare := values[0]
	for _, v := range values[1:] {
		if v != compare {
			return false
		}
	}
	return true
}

// TrueString returns true if the string contains a recognized true value, such
// as "true", "True", "TRUE", "yes", "on", etc. Yes, there is already
// strconv.ParseBool, but this function is often easier to work with since it
// just returns true or false instead of (bool, error) like ParseBool does. If
// you need to differentiate between true, false, and unknown, ParseBool should
// be your choice. Although I suppose you could use TrueString(s),
// FalseString(s), and !TrueString(s) && !FalseString(s).
func TrueString(value string) bool {
	v := strings.ToLower(value)
	switch v {
	case "true":
		return true
	case "yes":
		return true
	case "on":
		return true
	case "1":
		return true
	}
	return false
}

// FalseString returns true if the string contains a recognized false value,
// such as "false", "False", "FALSE", "no", "off", etc. Yes, there is already
// strconv.ParseBool, but this function is often easier to work with since it
// just returns true or false instead of (bool, error) like ParseBool does. If
// you need to differentiate between true, false, and unknown, ParseBool should
// be your choice. Although I suppose you could use TrueString(s),
// FalseString(s), and !TrueString(s) && !FalseString(s).
func FalseString(value string) bool {
	v := strings.ToLower(value)
	switch v {
	case "false":
		return true
	case "no":
		return true
	case "off":
		return true
	case "0":
		return true
	}
	return false
}
