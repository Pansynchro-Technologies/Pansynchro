using System;
using System.Collections.Generic;
using System.Text;

namespace Pansynchro.Core.Pansync
{
    partial class PansyncLexer
    {
        protected int _skipWhitespaceRegion = 0;

        private int _begin => _tokenStartCharIndex;

        private StringBuilder text = new StringBuilder();

        private bool SkipWhitespace => _skipWhitespaceRegion > 0;

        private void setText(string text)
        {
            Text = text;
        }
    }
}