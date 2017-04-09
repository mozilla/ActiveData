#A Standard Chart Schema
chart definition

## ``area` Property
details regarding the plot area, but nothing about the series

  * `area.style` - *object*
    * `area.style.style` - *string* css line style descriptions
    * `area.style.format` - *string* text format string
    * `area.style.color` - *string* css color
    * `area.style.visibility` - *string* initial visibility
    * `area.style.height` - *number* css height
    * `area.style.padding` - *string* css padding
    * `area.style.width` - *number* css width
    * `area.style.z-index` - *number* css z-depth
    * `area.style.line` - *object*
      * `area.style.line.style` - *string* css line style descriptions
      * `area.style.line.format` - *string* text format string
      * `area.style.line.color` - *string* css color
      * `area.style.line.visibility` - *string* initial visibility
      * `area.style.line.height` - *number* css height
      * `area.style.line.padding` - *string* css padding
      * `area.style.line.width` - *number* css width
      * `area.style.line.z-index` - *number* css z-depth
      * `area.style.line.font` - *string* css font
      * `area.style.line.border` - *string* css border
    * `area.style.font` - *string* css font
    * `area.style.border` - *string* css border
## `axis` Property
list of axis, property names not limited to 'x' and 'y', 

  * `axis.y` - *object* details on a visual axis
    * `axis.y.format` - *string* format of the reference values on the axis
    * `axis.y.rug` - *boolean* show projection as a series of ticks along the axis
    * `axis.y.value` - *string* expression to evaluate, or name of the property
    * `axis.y.label` - *string* name of axis
    * `axis.y.range` - *object* define the range of values on axis
      * `axis.y.range.max` - *number* maximum axis value shown
      * `axis.y.range.showZero` - *bolean* show zero coordinate, even if calculated range would not
      * `axis.y.range.min` - *number* minimum axis value shown
    * `axis.y.normalized` - *boolean* Convert to % of total
    * `axis.y.position` - *string* where to place the axis, relative to plot area (top/right/bottom/left)
    * `axis.y.unit` - *string* the measurement unit, using multiply (`*`) and divide (`/`) operators
  * `axis.x` - *object* details on a visual axis
    * `axis.x.format` - *string* format of the reference values on the axis
    * `axis.x.rug` - *boolean* show projection as a series of ticks along the axis
    * `axis.x.value` - *string* expression to evaluate, or name of the property
    * `axis.x.label` - *string* name of axis
    * `axis.x.range` - *object* define the range of values on axis
      * `axis.x.range.max` - *number* maximum axis value shown
      * `axis.x.range.showZero` - *bolean* show zero coordinate, even if calculated range would not
      * `axis.x.range.min` - *number* minimum axis value shown
    * `axis.x.normalized` - *boolean* Convert to % of total
    * `axis.x.position` - *string* where to place the axis, relative to plot area (top/right/bottom/left)
    * `axis.x.unit` - *string* the measurement unit, using multiply (`*`) and divide (`/`) operators
## `data` Property
an array of objects

## `legend` Property
more configuration for legend

  * `legend.position` - *string* position of legend relative to plot area (top/left/bottom/right)
  * `legend.style` - *object*
    * `legend.style.style` - *string* css line style descriptions
    * `legend.style.format` - *string* text format string
    * `legend.style.color` - *string* css color
    * `legend.style.visibility` - *string* initial visibility
    * `legend.style.height` - *number* css height
    * `legend.style.padding` - *string* css padding
    * `legend.style.width` - *number* css width
    * `legend.style.z-index` - *number* css z-depth
    * `legend.style.line` - *object*
      * `legend.style.line.style` - *string* css line style descriptions
      * `legend.style.line.format` - *string* text format string
      * `legend.style.line.color` - *string* css color
      * `legend.style.line.visibility` - *string* initial visibility
      * `legend.style.line.height` - *number* css height
      * `legend.style.line.padding` - *string* css padding
      * `legend.style.line.width` - *number* css width
      * `legend.style.line.z-index` - *number* css z-depth
      * `legend.style.line.font` - *string* css font
      * `legend.style.line.border` - *string* css border
    * `legend.style.font` - *string* css font
    * `legend.style.border` - *string* css border
  * `legend.label` - *string* name the legend
## `series` Property
what is ploted 

  * `series.hoverStyle` - *object* for when hovering over datapoint
    * `series.hoverStyle.symbol` - *string* shape while hovering
    * `series.hoverStyle.style` - *object*
      * `series.hoverStyle.style.style` - *string* css line style descriptions
      * `series.hoverStyle.style.format` - *string* text format string
      * `series.hoverStyle.style.color` - *string* css color
      * `series.hoverStyle.style.visibility` - *string* initial visibility
      * `series.hoverStyle.style.height` - *number* css height
      * `series.hoverStyle.style.padding` - *string* css padding
      * `series.hoverStyle.style.width` - *number* css width
      * `series.hoverStyle.style.z-index` - *number* css z-depth
      * `series.hoverStyle.style.line` - *object*
        * `series.hoverStyle.style.line.style` - *string* css line style descriptions
        * `series.hoverStyle.style.line.format` - *string* text format string
        * `series.hoverStyle.style.line.color` - *string* css color
        * `series.hoverStyle.style.line.visibility` - *string* initial visibility
        * `series.hoverStyle.style.line.height` - *number* css height
        * `series.hoverStyle.style.line.padding` - *string* css padding
        * `series.hoverStyle.style.line.width` - *number* css width
        * `series.hoverStyle.style.line.z-index` - *number* css z-depth
        * `series.hoverStyle.style.line.font` - *string* css font
        * `series.hoverStyle.style.line.border` - *string* css border
      * `series.hoverStyle.style.font` - *string* css font
      * `series.hoverStyle.style.border` - *string* css border
    * `series.hoverStyle.size` - *number* size while hovering
  * `series.value` - *string* expression to extract from data and chart, use tuple if plotting more than one dimension
  * `series.label` - *string* name of the series
  * `series.marker` - *object* single-value mark on one axis only
    * `series.marker.symbol` - *string* shape of datapoint
    * `series.marker.style` - *object*
      * `series.marker.style.style` - *string* css line style descriptions
      * `series.marker.style.format` - *string* text format string
      * `series.marker.style.color` - *string* css color
      * `series.marker.style.visibility` - *string* initial visibility
      * `series.marker.style.height` - *number* css height
      * `series.marker.style.padding` - *string* css padding
      * `series.marker.style.width` - *number* css width
      * `series.marker.style.z-index` - *number* css z-depth
      * `series.marker.style.line` - *object*
        * `series.marker.style.line.style` - *string* css line style descriptions
        * `series.marker.style.line.format` - *string* text format string
        * `series.marker.style.line.color` - *string* css color
        * `series.marker.style.line.visibility` - *string* initial visibility
        * `series.marker.style.line.height` - *number* css height
        * `series.marker.style.line.padding` - *string* css padding
        * `series.marker.style.line.width` - *number* css width
        * `series.marker.style.line.z-index` - *number* css z-depth
        * `series.marker.style.line.font` - *string* css font
        * `series.marker.style.line.border` - *string* css border
      * `series.marker.style.font` - *string* css font
      * `series.marker.style.border` - *string* css border
    * `series.marker.size` - *number* size of the datapoint
  * `series.type` - *string* the chart type to show as (bar/line/dot)
  * `series.axis` - *string* name of the axis to apply against: can be any #chart.axis property name. Use tuple if plotting more than one dimension.
## `target` (string)
name of dom elements to insert chart
## `title` Property
details regarding the title. Can also be a simple string.

  * `title.position` - *string* location of title relative to area (default=top)
  * `title.description` - *string* detail text shown while hovering over title (default=null)
  * `title.label` - *string* actual text of the title
