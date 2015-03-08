package varint

import (
	"bytes"
	"testing"
)

func TestReadBytes_GoodData_5Bytes(t *testing.T) {
	// varint.ReadBytes expects a varint encoded int, followed by that many bytes
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(10))    // varint encoded 10 == 5
	buf.Write([]byte("total")) // 5 bytes

	outbytes, err := ReadBytes(buf)
	ok(t, err)
	equals(t, 5, len(outbytes))
	equals(t, "total", string(outbytes))
}

func TestReadBytes_GoodData_0Bytes(t *testing.T) {
	// 0 as the varint means no bytes follow
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(0)) // varint encoded 0 == 0

	outbytes, err := ReadBytes(buf)
	ok(t, err)
	assert(t, outbytes == nil, "outbytes should be nil")
}

func TestReadBytes_BadData_VarintEncodesNegativeNumber(t *testing.T) {
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(11)) // varint encoded 11 == -6

	// an error should be returned
	outbytes, err := ReadBytes(buf)
	assert(t, err != nil, "err should not nil")
	assert(t, outbytes == nil, "outbytes should be nil")
}

func TestReadString_GoodData(t *testing.T) {
	// varint.ReadBytes expects a varint encoded int, followed by that many bytes
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(12))     // varint encoded 12 == 6
	buf.Write([]byte("ZAXXON")) // 6 bytes

	outstr, err := ReadString(buf)
	ok(t, err)
	equals(t, 6, len(outstr))
	equals(t, "ZAXXON", outstr)
}

func TestReadString_Empty(t *testing.T) {
	// varint.ReadBytes expects a varint encoded int, followed by that many bytes
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(0)) // varint encoded 12 == 6

	outstr, err := ReadString(buf)
	ok(t, err)
	equals(t, "", outstr)
}

func TestReadString_BadData_NegativeVarint(t *testing.T) {
	// varint.ReadBytes expects a varint encoded int, followed by that many bytes
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(77)) // odd varint encode negative numbers, which shouldn't happen when encoding strings

	outstr, err := ReadString(buf)
	assert(t, err != nil, "err should not nil")
	equals(t, "", outstr)
}

func TestReadString_LargeString(t *testing.T) {
	/* ---[ setup ]--- */
	strlen := int32(len(largeString))
	buf := new(bytes.Buffer)

	// the encoded varint will be 2 bytes in length
	err := EncodeAndWriteVarInt32(buf, strlen)
	ok(t, err)

	_, err = buf.WriteString(largeString)
	ok(t, err)

	/* ---[ code under test ]--- */
	outstr, err := ReadString(buf)
	ok(t, err)
	equals(t, int(strlen), len(outstr))
	equals(t, largeString, outstr)
}

var largeString = `
For a number of years I have been familiar with the observation that the quality of programmers is a decreasing function of the density of go to statements in the programs they produce. More recently I discovered why the use of the go to statement has such disastrous effects, and I became convinced that the go to statement should be abolished from all "higher level" programming languages (i.e. everything except, perhaps, plain machine code). At that time I did not attach too much importance to this discovery; I now submit my considerations for publication because in very recent discussions in which the subject turned up, I have been urged to do so.

My first remark is that, although the programmer's activity ends when he has constructed a correct program, the process taking place under control of his program is the true subject matter of his activity, for it is this process that has to accomplish the desired effect; it is this process that in its dynamic behavior has to satisfy the desired specifications. Yet, once the program has been made, the "making' of the corresponding process is delegated to the machine.

My second remark is that our intellectual powers are rather geared to master static relations and that our powers to visualize processes evolving in time are relatively poorly developed. For that reason we should do (as wise programmers aware of our limitations) our utmost to shorten the conceptual gap between the static program and the dynamic process, to make the correspondence between the program (spread out in text space) and the process (spread out in time) as trivial as possible.

Let us now consider how we can characterize the progress of a process. (You may think about this question in a very concrete manner: suppose that a process, considered as a time succession of actions, is stopped after an arbitrary action, what data do we have to fix in order that we can redo the process until the very same point?) If the program text is a pure concatenation of, say, assignment statements (for the purpose of this discussion regarded as the descriptions of single actions) it is sufficient to point in the program text to a point between two successive action descriptions. (In the absence of go to statements I can permit myself the syntactic ambiguity in the last three words of the previous sentence: if we parse them as "successive (action descriptions)" we mean successive in text space; if we parse as "(successive action) descriptions" we mean successive in time.) Let us call such a pointer to a suitable place in the text a "textual index."

When we include conditional clauses (if B then A), alternative clauses (if B then A1 else A2), choice clauses as introduced by C. A. R. Hoare (case[i] of (A1, A2,···, An)),or conditional expressions as introduced by J. McCarthy (B1 -> E1, B2 -> E2, ···, Bn -> En), the fact remains that the progress of the process remains characterized by a single textual index.

As soon as we include in our language procedures we must admit that a single textual index is no longer sufficient. In the case that a textual index points to the interior of a procedure body the dynamic progress is only characterized when we also give to which call of the procedure we refer. With the inclusion of procedures we can characterize the progress of the process via a sequence of textual indices, the length of this sequence being equal to the dynamic depth of procedure calling.

Let us now consider repetition clauses (like, while B repeat A or repeat A until B). Logically speaking, such clauses are now superfluous, because we can express repetition with the aid of recursive procedures. For reasons of realism I don't wish to exclude them: on the one hand, repetition clauses can be implemented quite comfortably with present day finite equipment; on the other hand, the reasoning pattern known as "induction" makes us well equipped to retain our intellectual grasp on the processes generated by repetition clauses. With the inclusion of the repetition clauses textual indices are no longer sufficient to describe the dynamic progress of the process. With each entry into a repetition clause, however, we can associate a so-called "dynamic index," inexorably counting the ordinal number of the corresponding current repetition. As repetition clauses (just as procedure calls) may be applied nestedly, we find that now the progress of the process can always be uniquely characterized by a (mixed) sequence of textual and/or dynamic indices.

The main point is that the values of these indices are outside programmer's control; they are generated (either by the write-up of his program or by the dynamic evolution of the process) whether he wishes or not. They provide independent coordinates in which to describe the progress of the process.

Why do we need such independent coordinates? The reason is - and this seems to be inherent to sequential processes - that we can interpret the value of a variable only with respect to the progress of the process. If we wish to count the number, n say, of people in an initially empty room, we can achieve this by increasing n by one whenever we see someone entering the room. In the in-between moment that we have observed someone entering the room but have not yet performed the subsequent increase of n, its value equals the number of people in the room minus one!`
