import { Command } from '@commander-js/extra-typings'
import chalk from 'chalk'
import ora from 'ora'
import pkg from '../package.json'
import { tryPrompt } from './helpers'
import { csvCommand } from './subcommands/csvCommand'
import { excelCommamd } from './subcommands/excelCommand'

export const spinner = ora({
  hideCursor: false,
  discardStdin: false,
})

export const program = new Command(pkg.name).version(pkg.version)
.description('A CLI tool to parse, filter and split Excel and CSV files and write the results to new CSV files of a specified size')
.showSuggestionAfterError(true)
.configureHelp({ sortSubcommands: true })
.addCommand(excelCommamd)
.addCommand(csvCommand)

program.parse(process.argv)
async function updateCommandOptions(command, globalOptions) {
  for (const commandOption of command.options) {
    const attributeName = commandOption.attributeName() as keyof typeof globalOptions

    const val = command.getOptionValue(attributeName)

    const source = command.getOptionValueSource(attributeName)

    if (typeof source !== 'undefined' && source !== 'env') {
      const optionMessage = `Should ${chalk.yellowBright(commandOption.long)} be set to ${chalk.cyanBright(val)}?\n(${commandOption.description})`

      const [, setValueAnswer] = await tryPrompt('confirm', {
        message: optionMessage,
        default: true,

      })

      if (setValueAnswer === false) {
        if (commandOption.argChoices) {
          const [, optionValue] = await tryPrompt('select', {
            message: `${chalk.yellowBright(commandOption.long)} (${commandOption.description})`,
            choices: commandOption.argChoices,
            default: val,
          })

          // globalOptions[attributeName] = optionValue
        }
        else if (typeof val === 'boolean') {
          const [, optionValue] = await tryPrompt('select', {
            message: `${chalk.yellowBright(commandOption.long)} (${commandOption.description})`,
            default: val,
            choices: [{
              name: 'true',
              value: true,
            }, {
              name: 'false',
              value: false,
            }],
          })

          // globalOptions[attributeName] = optionValue
        }
        else {
          const [, optionValue] = await tryPrompt('input')({ message: `${chalk.yellowBright(commandOption.long)} (${commandOption.description})` })

          // globalOptions[attributeName] = optionValue
        }
      }
    }
  }
}
