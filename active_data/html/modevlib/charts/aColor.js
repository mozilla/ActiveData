importScript("../util/convert.js");


Color = function (L, h, s) {
	this.L = L;
	this.h = h;
	this.s = s;
};

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
	var hex_color = [
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
		return hex({"r":this.rgb[0], "g":this.rgb[1], "b":this.rgb[2]});
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
