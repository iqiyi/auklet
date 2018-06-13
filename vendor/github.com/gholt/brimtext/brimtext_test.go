package brimtext

import (
	"fmt"
	"math"
	"sort"
	"testing"
)

func TestOrdinal(t *testing.T) {
	for i, v := range map[int]string{
		0: "th", 1: "st", 2: "nd", 3: "rd", 4: "th",
		10: "th", 11: "th", 12: "th", 13: "th", 14: "th",
		20: "th", 21: "st", 22: "nd", 23: "rd", 24: "th",
		100: "th", 101: "st", 102: "nd", 103: "rd", 104: "th",
		110: "th", 111: "th", 112: "th", 113: "th", 114: "th",
		120: "th", 121: "st", 122: "nd", 123: "rd", 124: "th",
	} {
		if OrdinalSuffix(i) != v {
			t.Errorf("%#v != %#v", i, v)
		}
	}
}

func TestThousandsSep(t *testing.T) {
	for i, x := range map[int64]string{
		-1000:               "-1,000",
		-1:                  "-1",
		0:                   "0",
		999:                 "999",
		1000:                "1,000",
		100000:              "100,000",
		1000000:             "1,000,000",
		1000000000000000000: "1,000,000,000,000,000,000",
	} {
		o := ThousandsSep(i, ",")
		if o != x {
			t.Errorf("ThousandsSep(%#v) %#v != %#v", i, o, x)
		}
	}
}

func TestThousandsSepU(t *testing.T) {
	for i, x := range map[uint64]string{
		0:                   "0",
		999:                 "999",
		1000:                "1,000",
		100000:              "100,000",
		1000000:             "1,000,000",
		1000000000000000000: "1,000,000,000,000,000,000",
	} {
		o := ThousandsSepU(i, ",")
		if o != x {
			t.Errorf("ThousandsSepU(%#v) %#v != %#v", i, o, x)
		}
	}
}

func TestHumanSize1000(t *testing.T) {
	for i, v := range map[float64]string{
		0:                         "0",
		1:                         "1",
		999:                       "999",
		1000:                      "1k",
		1020:                      "1.02k",
		1200:                      "1.2k",
		1230:                      "1.23k",
		1990:                      "1.99k",
		1994:                      "1.99k",
		1995:                      "2k",
		500000:                    "500k",
		999000:                    "999k",
		999001:                    "1m",
		999999:                    "1m",
		1000000:                   "1m",
		1000000000:                "1g",
		1000000000000:             "1t",
		1000000000000000:          "1p",
		1000000000000000000:       "1e",
		1000000000000000000000:    "1z",
		1000000000000000000000000: "1y",
		math.MaxFloat64:           "179769313486231511019805312187943965292154148682038588898066893456089697109784365701351847915848882651766062126140954855265208427307163165456491096260259379086558203821549391546479597946977280940974705083094024093930234524228095254815883037864908605183911465840386274717485710230683648y",
	} {
		o := HumanSize1000(i)
		if o != v {
			t.Errorf("HumanSize1000(%f) %s != %s", i, o, v)
		}
	}
}

func TestHumanSize1024(t *testing.T) {
	for i, v := range map[float64]string{
		0:                         "0",
		1:                         "1",
		999:                       "999",
		1000:                      "0.98K",
		1023:                      "1K",
		1024:                      "1K",
		1045:                      "1.02K",
		1229:                      "1.2K",
		1260:                      "1.23K",
		2038:                      "1.99K",
		2042:                      "1.99K",
		2043:                      "2K",
		512000:                    "500K",
		1022976:                   "999K",
		1022977:                   "0.98M",
		1048575:                   "1M",
		1048576:                   "1M",
		1073741824:                "1G",
		1099511627776:             "1T",
		1125899906842624:          "1P",
		1152921504606846976:       "1E",
		1180591620717411303424:    "1Z",
		1208925819614629174706176: "1Y",
		math.MaxFloat64:           "148701690847778289770602151825451107502507741524795492191598247302796499217268280270957376551963776483440625889732175596470614985269219355705721823651316423498954007077884209007184766014520893414163992189908860813599496489088150379189825745548561469601301424075524500984295881827155968Y",
	} {
		o := HumanSize1024(i)
		if o != v {
			t.Errorf("HumanSize1024(%f) %s != %s", i, o, v)
		}
	}
}

// func TestHumanSize1024(t *testing.T) {
// 	for i, v := range map[float64]string{
// 		0:                   "0",
// 		1:                   "1",
// 		512:                 "512",
// 		1023:                "1023",
// 		1024:                "1K",
// 		1535:                "1.5K",
// 		1536:                "1.5K",
// 		1048576:             "1M",
// 		1073741824:          "1G",
// 		1099511627776:       "1T",
// 		1125899906842624:    "1P",
// 		1152921504606846976: "1E",
// 	} {
// 		o := HumanSize1024(i)
// 		if o != v {
// 			t.Errorf("HumanSize1024(%f) %s != %s", i, o, v)
// 		}
// 	}
// }

func TestSentence(t *testing.T) {
	for in, exp := range map[string]string{
		"":          "",
		"testing":   "Testing.",
		"'testing'": "'testing'.",
		"Testing.":  "Testing.",
		"testing.":  "Testing.",
	} {
		out := Sentence(in)
		if out != exp {
			t.Errorf("Sentence(%#v) %#v != %#v", in, out, exp)
		}
	}
}

func TestStringSliceToLowerSort(t *testing.T) {
	out := []string{"DEF", "abc"}
	sort.Sort(StringSliceToLowerSort(out))
	exp := []string{"abc", "DEF"}
	for i := 0; i < len(out); i++ {
		if out[i] != exp[i] {
			t.Fatalf("StringSliceToLowerSort fail at index %d %#v != %#v", i, out[i], exp[i])
			return
		}
	}
	out = []string{"DEF", "abc"}
	sort.Strings(out)
	exp = []string{"DEF", "abc"}
	for i := 0; i < len(out); i++ {
		if out[i] != exp[i] {
			t.Fatalf("sort.Strings sort fail at index %d %#v != %#v", i, out[i], exp[i])
			return
		}
	}
}

func TestWrapUnicode(t *testing.T) {
	in := "Just a test sentence with Unicode \u0041 \u00c0 \uff21 \U0001d400 characters."
	out := Wrap(in, 12, "", "")
	exp := `Just a test
sentence
with Unicode
A À Ａ 𝐀
characters.`
	if out != exp {
		t.Errorf("Wrap(%#v) %#v != %#v", in, out, exp)
	}
}

func TestWrap(t *testing.T) {
	in := ""
	out := Wrap(in, 79, "", "")
	exp := ""
	if out != exp {
		t.Errorf("Wrap(%#v) %#v != %#v", in, out, exp)
	}
	in = "Just a test sentence."
	out = Wrap(in, 10, "", "")
	exp = `Just a
test
sentence.`
	if out != exp {
		t.Errorf("Wrap(%#v) %#v != %#v", in, out, exp)
	}
	in = "Just   a   test   sentence."
	out = Wrap(in, 10, "", "")
	exp = `Just a
test
sentence.`
	if out != exp {
		t.Errorf("Wrap(%#v) %#v != %#v", in, out, exp)
	}
	in = fmt.Sprintf("Just a %stest%s sentence.", string(ANSIEscape.Bold), string(ANSIEscape.Reset))
	out = Wrap(in, 10, "", "")
	exp = fmt.Sprintf(`Just a
%stest%s
sentence.`, string(ANSIEscape.Bold), string(ANSIEscape.Reset))
	if out != exp {
		t.Errorf("Wrap(%#v) %#v != %#v", in, out, exp)
	}
	in = "Just a test sentence."
	out = Wrap(in, 10, "1234", "5678")
	exp = `1234Just a
5678test
5678sentence.`
	if out != exp {
		t.Errorf("Wrap(%#v) %#v != %#v", in, out, exp)
	}
	in = `Just a test sentence. With
a follow on sentence.

And a separate paragraph.`
	out = Wrap(in, 10, "", "")
	exp = `Just a
test
sentence.
With a
follow on
sentence.

And a
separate
paragraph.`
	if out != exp {
		t.Errorf("Wrap(%#v) %#v != %#v", in, out, exp)
	}
	in = `Just a test sentence.  With     
          a follow           on sentence.

                And a separate  paragraph.       `
	out = Wrap(in, 10, "", "")
	exp = `Just a
test
sentence.
With a
follow on
sentence.

And a
separate
paragraph.`
	if out != exp {
		t.Errorf("Wrap(%#v) %#v != %#v", in, out, exp)
	}
}

func TestAllEqual(t *testing.T) {
	if !AllEqual() {
		t.Fatal("")
	}
	if !AllEqual([]string{}...) {
		t.Fatal("")
	}
	if !AllEqual("bob") {
		t.Fatal("")
	}
	if !AllEqual("bob", "bob") {
		t.Fatal("")
	}
	if !AllEqual("bob", "bob", "bob") {
		t.Fatal("")
	}
	if !AllEqual([]string{"bob", "bob", "bob"}...) {
		t.Fatal("")
	}
	if AllEqual("bob", "sue") {
		t.Fatal("")
	}
	if AllEqual("bob", "bob", "sue") {
		t.Fatal("")
	}
	if AllEqual([]string{"bob", "bob", "sue"}...) {
		t.Fatal("")
	}
}

func TestTrueString(t *testing.T) {
	if TrueString("") {
		t.Fatal("")
	}
	if TrueString("slkdfjsdkfj") {
		t.Fatal("slkdfjsdkfj")
	}
	if !TrueString("true") {
		t.Fatal("true")
	}
	if !TrueString("True") {
		t.Fatal("True")
	}
	if !TrueString("TRUE") {
		t.Fatal("TRUE")
	}
	if !TrueString("TruE") {
		t.Fatal("TruE")
	}
	if !TrueString("on") {
		t.Fatal("on")
	}
	if !TrueString("yes") {
		t.Fatal("yes")
	}
	if !TrueString("1") {
		t.Fatal("1")
	}
	if TrueString("-1") {
		t.Fatal("-1")
	}
	if TrueString("2") {
		t.Fatal("2")
	}
}

func TestFalseString(t *testing.T) {
	if FalseString("") {
		t.Fatal("")
	}
	if FalseString("slkdfjsdkfj") {
		t.Fatal("slkdfjsdkfj")
	}
	if !FalseString("false") {
		t.Fatal("false")
	}
	if !FalseString("False") {
		t.Fatal("False")
	}
	if !FalseString("FALSE") {
		t.Fatal("FALSE")
	}
	if !FalseString("FalSe") {
		t.Fatal("FalSe")
	}
	if !FalseString("off") {
		t.Fatal("off")
	}
	if !FalseString("no") {
		t.Fatal("no")
	}
	if !FalseString("0") {
		t.Fatal("0")
	}
	if FalseString("-1") {
		t.Fatal("-1")
	}
	if FalseString("2") {
		t.Fatal("2")
	}
}
