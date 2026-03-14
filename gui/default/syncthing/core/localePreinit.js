(function () {
    'use strict';

    var forcedLocale = 'zh-CN';
    var elementNode = 1;
    var textNode = 3;

    function flattenTranslations(source, target, prefix) {
        Object.keys(source || {}).forEach(function (key) {
            var value = source[key];
            var path = prefix ? prefix + '.' + key : key;

            if (value && typeof value === 'object' && !Array.isArray(value)) {
                flattenTranslations(value, target, path);
                return;
            }

            target[path] = value;
        });
    }

    function convertPlaceholders(text) {
        return String(text)
            .replace(/\{\{/g, '{%')
            .replace(/\}\}/g, '%}');
    }

    function translatedTextNode(element) {
        var translatedNode = null;

        for (var i = 0; i < element.childNodes.length; i++) {
            var node = element.childNodes[i];
            if (node.nodeType === elementNode) {
                return null;
            }
            if (node.nodeType === textNode && node.nodeValue.trim()) {
                if (translatedNode) {
                    return null;
                }
                translatedNode = node;
            }
        }

        return translatedNode;
    }

    function replaceFallback(element, translations) {
        var textNode = translatedTextNode(element);
        if (!textNode) {
            return;
        }

        var original = textNode.nodeValue;
        var key = element.getAttribute('translate') || original.trim();
        var translated = translations[key];

        if (!translated) {
            return;
        }

        if (!element.getAttribute('translate')) {
            element.setAttribute('translate', key);
        }

        var start = original.indexOf(original.trim());
        var end = start + original.trim().length;
        textNode.nodeValue = original.slice(0, start) + convertPlaceholders(translated) + original.slice(end);
    }

    function loadTranslations() {
        var request = new XMLHttpRequest();
        request.open('GET', 'assets/lang/lang-' + forcedLocale + '.json', false);
        request.send(null);

        if (request.status >= 200 && request.status < 300) {
            return JSON.parse(request.responseText);
        }

        return null;
    }

    try {
        document.documentElement.setAttribute('lang', forcedLocale);

        var rawTranslations = loadTranslations();
        if (rawTranslations) {
            var translations = {};
            flattenTranslations(rawTranslations, translations, '');

            Array.prototype.forEach.call(document.querySelectorAll('[translate]'), function (element) {
                replaceFallback(element, translations);
            });
        }
    } catch (error) {
        if (window.console && console.warn) {
            console.warn('Failed to preinitialize Chinese GUI locale.', error);
        }
    } finally {
        document.documentElement.classList.remove('locale-preinit');
    }
}());
