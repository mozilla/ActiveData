importScript("../util/convert.js");


Color = function (L, h, s) {
	this.L = L;
	this.h = h;
	this.s = s;
};

var TEST_COLOR;

(function () {
	// MOTIVATION
	// ----------
	//
	// EVERY COLOUR LIBRARY I HAVE TRIED BARELY DOES ONE OF THESE, NEVER MIND
	// TWO OF THEM:
	// * LINEAR AMPLIFICATION OF LIGHTNESS (linear defined by cie)
	// * NATURAL ROTATION IN HUE
	// * VARY SATURATION WITHOUT CONTORTING THE HUE
	// * REASONABLE APPROXIMATIONS FOR COLOUR OUTSIDE THE sRGB GAMUT!!!
	//
	// I AM SURE THE CIE* FAMILY HAS A TRANSFORMATION (?CIELuv?) BUT
	// I AM HARD PRESSED TO FIND AN IMPLEMENTATION THAT WORKS.  THE LIBRARIES
	// OUT THERE ARE VERY NAIVE!!
	//
	// IN THE MEANTIME, I WILL IMPLEMENT THE FIRST TWO WITH LINEAR INTERPOLATION
	// OF A FINE COLOR-WHEEL MESH.
	//
	// ...AFTER AN HOUR OF LOOKING, THIS IS THE ONLY NATURAL LOOKING COLOUR
	// WHEEL THANKS TO http://www.realcolorwheel.com/colorwheel.htm
	//
	//      19        20        21        22        23        24        25        26        27        28        29        30        31        32        33        34        35        36         1         2         3         4         5         6         7         8         9        10        11        12        13        14        15        16        17        18
	TEST_COLOR = [
		["e2ecf8", "dff0fa", "dff0fa", "e2f0fb", "e7f5fe", "e7f5fe", "e7f5fe", "e6f5fa", "e6f4f5", "e6f5f2", "e1f2ec", "e2f1ea", "e2f2e8", "e5f3e6", "e8f3e3", "eff6e4", "f6f8e2", "fcfbdf", "fcfbdf", "fcfbdf", "fff8da", "fff4e2", "fff3e5", "fff0e9", "feebef", "feebef", "feebef", "fdedf0", "fdecf4", "fdecf4", "fdecf4", "f8edf5", "f7ecf4", "f2eaf5", "ece9f4", "e9e7f4", "e8e6f3"],
		["dbe4f5", "d5e6f6", "d2e7f8", "d3edfa", "d3effb", "d5effc", "d5effc", "d3eef7", "d4edf1", "d3ece9", "cee9e0", "cee9da", "cde8d7", "d1ead7", "daecd4", "e3f0d2", "eef2cd", "faf7ca", "fcf8c9", "fcf8c9", "fff3bf", "ffecce", "ffebd3", "fee6da", "fbdce2", "fbdce2", "fcdde5", "fcdee6", "fcdeea", "fbdfeb", "fde1ed", "f6e0ed", "f0deec", "e8dbec", "dfd7ec", "d9d6eb", "d8d6eb"],
		["c3d3ed", "c5d7ef", "bbd5ee", "b6d8f3", "b1dbf4", "b0dcf5", "b4e4fb", "bee6f2", "bce4e6", "bae1dc", "bae0d1", "bbe1cc", "b7dfc7", "bddfc4", "c5e3c1", "d6e8c0", "e5edba", "f3f2b9", "fbf7ba", "fff8ba", "fee9b0", "fedbb5", "ffdcc6", "fbcbc1", "f9bdc9", "f8becc", "f9c1ce", "f9c6d5", "f9c9d8", "f9cce0", "facde1", "f2cbe0", "eac8e0", "ddc6e0", "d0c3e1", "c5c0e0", "c1c0e0"],
		["9db3dc", "a5bde3", "a8cbeb", "a9cded", "9fcdee", "9fd6f4", "a0ddf9", "a1ddf5", "a5ddec", "acdde1", "a9dad4", "a7d8c3", "abd9bc", "add9b6", "b6dcb3", "c8e2b2", "dceaaf", "eff0ae", "fef6ad", "fff1b2", "fee3ae", "fdd6af", "fed1ba", "fabfb7", "f8b2bc", "f8b4bf", "f9b7c5", "f7b7c8", "f8bbcd", "f7bbd4", "f8bcd6", "eeb6d3", "e1b4d5", "cfafd4", "beacd4", "b0a7d2", "abaad4"],
		["8093cb", "84a0d2", "98bbe3", "94c1e8", "8ec6eb", "8ed0f3", "8bd7f9", "97d8ea", "97d7e1", "97d4cc", "98d3bd", "9ad3b3", "9ad2ae", "a2d4af", "b0daac", "c0deaa", "d7e7a9", "ebefa5", "fff6a7", "fee9a6", "ffe0a7", "fdd0a7", "fbcbb4", "fbbdb1", "f7aeb7", "f6adb8", "f7afbd", "f7aec1", "f7afc7", "f8afcc", "f5aece", "edacce", "dca8ce", "c7a2cd", "b19ecc", "a29acb", "a09cce"],
		["6277ba", "6585c2", "7ea3d7", "75adde", "77bbe8", "73c8f1", "6bcef7", "7dd0e2", "7fcdcd", "84cebf", "87cdb3", "8acba9", "88cba0", "93cea0", "a3d49d", "b5d99c", "cde29d", "e8ec98", "fff699", "ffea9b", "ffdd9b", "fccc9c", "fbc4a6", "f9b0a1", "f59ea6", "f59ea7", "f59eae", "f49fb4", "f59fbc", "f5a0c1", "f59fc4", "ea9dc5", "d899c4", "c196c4", "a991c5", "968dc4", "8f8cc5"],
		["4763ad", "4a72b8", "6088c6", "5c9dd3", "51b1e4", "46bfee", "4bc8f4", "5cc9e0", "69c8ca", "6cc7b8", "6dc6aa", "6fc59e", "73c594", "85ca94", "97cf92", "afd791", "d1e397", "e7eb8c", "fff589", "ffeb8c", "ffdb8b", "fdc98f", "faba94", "f8a794", "f68e97", "f48e99", "f48f9f", "f58fa7", "f491b0", "f391b8", "f28fbb", "eb8ebb", "d78bbc", "bc87bb", "a182bb", "8a80bd", "7e7ebc"],
		["3053a4", "2d65b0", "3d76bb", "4c94cf", "3da8e0", "26b8eb", "0dc3f3", "01c0ea", "34c0cf", "3fbda7", "49bd9a", "4ebc8d", "50bc82", "68c181", "8ccb85", "a6d283", "cde18c", "e2e97f", "fff57c", "fee57e", "ffd179", "fcbf80", "f8a677", "f6967e", "f27e81", "f37a81", "f47785", "f27a93", "f37d9f", "f27da9", "f176ad", "e57aae", "cd78b1", "b074b0", "9977b4", "7f74b6", "7071b4"],
		["204aa0", "2961ae", "0a66b1", "1582c5", "0098d7", "00ade7", "01baf2", "01bad8", "01babf", "00b7a5", "01b693", "00b47f", "34b676", "54bc71", "79c36c", "99cd6b", "c8dc69", "dee55b", "fef455", "fbd350", "fdbd51", "faa555", "f88f58", "f37755", "f15e57", "f15c5e", "f05c6a", "f15d77", "f16185", "f05d93", "ee509d", "de5d9e", "c65da0", "a65aa2", "865ca4", "655aa8", "5059a8"],
		["21439c", "1951a4", "0061ae", "0070ba", "0186cb", "009fe1", "01b0ef", "00b0d7", "00adb3", "00ab95", "00aa7d", "00a968", "01ad61", "01b055", "52b950", "7dc148", "bed73d", "dce335", "fff218", "f9c726", "fcae28", "f78f2e", "f3742f", "f1552d", "ee3532", "ee303a", "ee314d", "ef315d", "ee306e", "ed2c7f", "ed2a92", "d7308c", "bf348f", "9e3c93", "764297", "49459a", "2e469e"],
		["22409a", "0e4aa0", "0057a7", "0064b2", "0079c2", "0191d7", "00adef", "00abc7", "00a9a2", "00a784", "01a66e", "00a65c", "00a54f", "00ab4f", "1db24c", "61bc47", "a8cf38", "cfdd28", "fef200", "f8c21a", "f9991e", "f47820", "f25d23", "ee3f22", "ed1b24", "ee1b2e", "ed1a3b", "ed174b", "ed135f", "ee0874", "ed008c", "d11786", "c03590", "8c338f", "663a93", "3d3e98", "22409a"],
		["233d94", "1b4398", "0057a4", "0060ac", "016db5", "0085ca", "009de0", "009cc5", "009f9a", "009e85", "009f79", "009d58", "009d4e", "009b48", "47a746", "64ab43", "88b33d", "bac430", "f8d308", "f8b31a", "f19120", "ec7322", "e65923", "e63525", "e51e25", "e31f2d", "e21b42", "e01b49", "df1758", "de0f79", "dd0c84", "cb1b87", "9e2d89", "87318a", "74368c", "463b91", "223d94"],
		["233a8a", "1a418e", "00549d", "005ba4", "0163aa", "017cc2", "0191d7", "0087b4", "008b90", "008e82", "008e76", "009053", "00904a", "009345", "4a9240", "699a3e", "7f9f3a", "9ba937", "eeb51d", "e89420", "e37b24", "df6824", "d45125", "d43026", "cf1e24", "ce1c28", "ca1e36", "c81d3d", "c61d46", "c3176d", "c01677", "af1e79", "8e2c7f", "7e2f80", "693484", "413889", "213a8c"],
		["233780", "1e3c85", "004d93", "005399", "005ca1", "0075bb", "0181c8", "0075ab", "007b8d", "007676", "007c71", "00804d", "008141", "0a8340", "44813c", "6e8439", "7d8a38", "8f9335", "e6a122", "da7c24", "d16826", "cd5b25", "bb4227", "bc2c24", "ba1f23", "ba202a", "b21e36", "b11e3b", "aa1c42", "a81d61", "a21f6b", "95226f", "812b76", "752d77", "652f7a", "3f337d", "223581"],
		["23347a", "1f377d", "08478c", "004c92", "005399", "006ab0", "016db5", "0069a3", "006b85", "016d7a", "006e6d", "007348", "01743d", "10753b", "426d35", "637134", "797933", "8c8132", "c08028", "cb6429", "c35b26", "b94826", "b74127", "ac2824", "ab2025", "a82128", "9f2035", "9e1f3a", "981f3e", "921f5a", "8f2265", "852263", "752a6f", "6d2a71", "602d72", "323175", "203177"],
		["223174", "1f367a", "114388", "05488e", "004f94", "005ba6", "005ca7", "005d96", "006380", "006473", "006764", "006a42", "006c38", "116333", "3e5f30", "586132", "6f6831", "71662e", "a46228", "a74f27", "a64727", "a93f27", "a43625", "992824", "961f23", "921f24", "8d1d2b", "891c31", "851a35", "811d53", "81205d", "6e2461", "71296b", "66296c", "592a6e", "372e71", "202f74"],
		["202e6d", "1f3171", "1d3678", "164186", "064997", "004f9e", "004e99", "004e7f", "00566f", "015965", "005a5b", "005b3a", "015c31", "0e5b2f", "3c592d", "4b532c", "5f532b", "5f532b", "824725", "7b3420", "863321", "893221", "872f21", "852320", "811d1d", "811c20", "801b29", "7e1b2f", "7d1c30", "711e48", "6c1c4f", "661e50", "622761", "5c2664", "542865", "302a6a", "202b6d"],
		["1d265f", "1b2a63", "18326d", "163671", "173979", "114287", "09458b", "0e4677", "0c4e68", "014f5c", "064f55", "055437", "00552e", "1d542d", "365029", "464c2a", "574a27", "574a27", "5b321e", "5e311e", "632e1e", "6c2a1e", "6c2a1e", "73231c", "731f1d", "731e20", "711d2c", "711c2f", "6e1c33", "681c42", "631c46", "601d49", "502052", "4a1f54", "402052", "28255e", "1a245f"],
		["192259", "18255c", "162b62", "142d65", "19316e", "15326c", "143771", "133c68", "123d60", "123e4b", "0e3d33", "0c3d28", "0d3e21", "163d20", "1c3d20", "2d3b21", "3a3a20", "47371e", "4e291a", "4f2a1a", "55291c", "5a2a1c", "5f2b1e", "62261e", "64231f", "622222", "63212d", "632031", "601f33", "5a1d3c", "591d3f", "551d40", "491e49", "431d4c", "3c2050", "242158", "18235b"],
		["131d50", "141e51", "142155", "152256", "19265d", "152155", "152354", "132553", "11274c", "102a37", "0e2b26", "0c2b1b", "0b2d15", "122b15", "172a14", "212a15", "2b2815", "332616", "462118", "441f16", "421d17", "3f1c16", "411c16", "421a18", "431a18", "43191a", "471923", "481a25", "471828", "44172c", "441630", "421732", "38193b", "341a41", "331a42", "211d4f", "161d53"],
		["131b4c", "131b4c", "131b4c", "131b4c", "131b4c", "131b4c", "111b4c", "121d4b", "111d43", "0e202e", "09201a", "061e0e", "061f09", "071e08", "111d09", "1d1a0b", "27160c", "30130f", "30110e", "30110e", "30110e", "30110f", "2f100e", "2f100e", "2f0f10", "301011", "321217", "35111d", "361122", "381124", "381126", "381229", "351431", "2e163a", "221741", "181a4a", "131b4c"],
	];

	var hex_color = [
		//  0         1         2         3         4         5          6        7         8         9         0         1         2         3         4         5          6        7         8         9         0         1         2         3         4         5          6        7         8         9          0         1         2         3         4         5
		["0000FF", "004DFF", "0077FF", "00A0FF", "00C4FF", "00E3FF", "4dfeff", "00FFDF", "00FFB2", "00FF8C", "00FF79", "00FF31", "00FF00", "17FF00", "70FF00", "A0FF00", "C8FF00", "EBFF00", "FFFF00", "FFDC00", "FFB000", "FF8400", "FF5D00", "FF3600", "FF0000", "FF0034", "FF006B", "FF0091", "FF00AA", "FF00D0", "FF00FF", "CC19FF", "9500F9", "7A00FF", "6100FF", "4700FF"],
		["0000EC", "0049F3", "0070F3", "0091E9", "00B8F3", "00D0EF", "24e3ec", "00EFD4", "00EDA8", "00E680", "00EE73", "00EE2D", "00e700", "16EE00", "68EB00", "94E900", "B8EB00", "DCED00", "FFEB01", "F6C800", "F09E00", "EC7700", "ED5300", "ED2E00", "df1b00", "EF072B", "E90068", "E60084", "df1d96", "EC00D9", "e020e4", "BB16EC", "8A00E6", "7000EA", "6100FF", "4700FF"],
		["0000D7", "0041DD", "0062D8", "0081D3", "009FDB", "00B4DA", "00CBDC", "00CDBE", "00D69A", "00CC74", "00D86A", "00DA29", "00D300", "15DA00", "5ED200", "89D500", "AEDC00", "CEE000", "FFD202", "EDB100", "EO8900", "D36700", "D74601", "D32201", "D50000", "D81633", "C70063", "CF0077", "D4008D", "CE00AE", "D100D1", "A913D6", "7E00D1", "6300CD", "5414d9", "4700FF"],
		["0000B3", "0037C6", "0056C1", "006FBF", "0087C3", "009BC6", "00AEC8", "00B2AB", "00BF8D", "00BB6C", "00C463", "00C324", "00C100", "14C300", "54B900", "7FC300", "9AC200", "BBC800", "FFC000", "E29500", "CE7100", "C85A00", "C03901", "BD1502", "C60404", "b8182a", "B80366", "B8006A", "BE007E", "B5009C", "BA00BA", "9310BA", "7000BA", "5812b7", "4c12c3", "3E00E7"],
		["0000AB", "002EAE", "004BAB", "015BBE", "0071AE", "0082B2", "008EB3", "009698", "00A57E", "00A965", "00A959", "00AA1E", "00AF00", "13AA00", "4AA000", "75B100", "89AD00", "A4AE00", "EC9F00", "DD8900", "C46200", "B74E00", "B53302", "B31103", "B00400", "a11726", "A80B54", "A6005F", "A5006E", "9D008C", "A600A6", "7F0DA4", "6100A0", "4E00A3", "420fa8", "3300C6"],
		["0000A0", "002593", "003E93", "01449E", "005996", "006A9E", "006D9D", "007D87", "008B6F", "009A5E", "009351", "008F18", "009F00", "139500", "448E00", "699B00", "759300", "8D9500", "CA7900", "B96A00", "AA5500", "954000", "952700", "9C0D01", "A50303", "8a1522", "98094C", "980057", "8D005E", "88007D", "920092", "611681", "530087", "4A0099", "38068e", "2800A5"],
		["00008E", "001C7C", "00327B", "023A8E", "004380", "004F89", "00538B", "006577", "007360", "008052", "007847", "007512", "008200", "127B00", "397300", "5A8200", "5E7400", "757000", "995100", "8C4800", "863A00", "792E00", "801F00", "820A00", "8F0303", "76141e", "81073F", "810049", "77004E", "74006F", "7C007C", "52066E", "43006C", "3F0082", "300978", "240981"],
		["00007D", "001263", "0f225c", "00266F", "002C6B", "003474", "003879", "004B6B", "005750", "006642", "00613E", "005D0E", "006800", "116000", "306000", "456A00", "495900", "4D4A00", "663800", "6E3100", "652300", "652100", "6F1900", "6E0901", "700101", "63141b", "611032", "730041", "670043", "5D005E", "600060", "42045B", "350054", "350071", "280663", "240563"],
		["0a045b", "00094C", "0a194c", "0a1950", "0a1950", "001C61", "102863", "002D51", "003C40", "004836", "004C36", "004D0A", "004A00", "0F4C00", "294C00", "354D00", "374200", "393200", "482100", "491300", "4D1300", "480F00", "4D0E00", "4D0500", "4E0101", "4A0A0F", "4A0222", "53002E", "570038", "4D0045", "480048", "34024A", "31004C", "2d0556", "240347", "1f0346"],
		["000042", "000540", "00103A", "060d41", "000544", "00054F", "001355", "00224A", "00313A", "00372E", "003A30", "003806", "003200", "0F3700", "213700", "233100", "2b2d04", "302302", "350F00", "350300", "350300", "350300", "350300", "350300", "350300", "360308", "370118", "37001E", "370024", "370032", "370037", "320040", "260039", "200035", "210233", "170039"],
		["000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000", "000000"]
	];

	var COLOR_MESH = hex_color.map(function (L) {
		return L.map(function (h) {
			return {
				"r": convert.hex2int(h.substring(0, 2)),
				"g": convert.hex2int(h.substring(2, 4)),
				"b": convert.hex2int(h.substring(4, 6))
			};
		});
	}).reverse();


	Color.RED = new Color(1.0, 240.0, 1.0);
	Color.GREEN = new Color(1.0, 120.0, 1.0);
	Color.BLUE = new Color(1.0, 0.0, 1.0);


	ColorSRGB = function (r, g, b) {

		if (r instanceof Array && g===undefined && b===undefined){
			this.rgb=r
		}else{
			this.rgb=[r, g, b];
		}
	};//function

	ColorSRGB.prototype.lighter=function(){
		return new ColorSRGB(this.rgb.map(function(v){
			return Math.min(Math.round(v+30), 255);
		}));
	};

	function toHTML(){
		try {
			return hex({"r": this.rgb[0], "g": this.rgb[1], "b": this.rgb[2]});
		}catch(e){
			return "#000000";
		}//try
	}
	ColorSRGB.prototype.toHTML=toHTML;
	ColorSRGB.prototype.toString=toHTML;

	Color.newInstance=function(value){
		if (value.startsWith("lhs(")){
			value=value.between("lhs(", ")");
			var lhs = value.split(",").map(function(v){return v.trim()-0;});
			return new Color(lhs[0], lhs[1], lhs[2]);
		}else if (value.startsWith("#")){
			var rgb = Array.newRange(0, 3).map(function(i){
				return convert.hex2int(value.substring(i*2+1, i*2+3))
			});
			return new ColorSRGB(rgb);
		}else{
			return value;  //MAYBE WE ARE LUCKY
		}//endif
	};

	Color.prototype.toString = function () {
		return "lhs(" + this.L + ", " + this.h + ", " + this.s + ")";
	};

	Color.newHTML=function(value){
		if (value.startsWith("rgb(")){
			value=value.between("rgb(", ")");
			var rgb = value.split(",").map(function(v){return v.trim()-0;});
			return new ColorSRGB(rgb[0], rgb[1], rgb[2]);
		}//endif

		//EXPECTING "#" AS FIRST VALUE
		return new ColorSRGB(
			convert.hex2int(value.substring(1, 3)),
			convert.hex2int(value.substring(3, 5)),
			convert.hex2int(value.substring(5, 7))
		);
	}//function


	Color.prototype.hue = function (degrees) {
		var h = this.h + degrees;
		while (h < 0.0) h += 360.0;
		while (h > 360.0) h -= 360.0;

		return new Color(this.L, h, this.s);
	};

	Color.prototype.multiply = function (amount) {
		var L = this.L * amount;
		return new Color(L, this.h, this.s);
	};

	Color.prototype.lighter = function(){
		return new Color(this.L * 1.2, this.h, this.s);
	};

	Color.prototype.darker = function(){
		return new Color(this.L / 1.2, this.h, this.s);
	};

	Color.prototype.toHTML = function () {
		if (this.L==1.0){
			var Lfloor = COLOR_MESH.length-1;
			var Lfloor_2 = Lfloor;
			var Lpart = 1.0;
		}else{
			var Lfloor = aMath.floor(this.L*COLOR_MESH.length);
			var Lfloor_2 = (Lfloor + 1) % COLOR_MESH.length;
			var Lpart = this.L*COLOR_MESH.length - Lfloor;
		}//endif

		var hfloor = aMath.floor(this.h / 10);
		var hfloor_2 = (hfloor + 1) % COLOR_MESH[Lfloor].length;
		var hpart = (this.h / 10) - hfloor;

		var x0 = COLOR_MESH[Lfloor][hfloor];
		var xH = COLOR_MESH[Lfloor][hfloor_2];
		var xL = COLOR_MESH[Lfloor_2][hfloor];
		var xHL = COLOR_MESH[Lfloor_2][hfloor_2];

		return hex(Map.zip(["r", "g", "b"].map(function (c) {
			return [c, x0[c] * (1 - Lpart) * (1 - hpart) + xH[c] * hpart * (1 - Lpart) + xL[c] * Lpart * (1 - hpart) + xHL[c] * hpart * Lpart];
		})));
	};

	function hex(value) {
		if (value===undefined){
			Log.error();
		}//endif
		return "#" + convert.int2hex(Math.round(value.r), 2) + convert.int2hex(Math.round(value.g), 2) + convert.int2hex(Math.round(value.b), 2);
	}//function

})();
