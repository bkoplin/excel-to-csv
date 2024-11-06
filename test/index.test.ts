import { homedir } from 'node:os'
import {
  select,
  Separator,
} from '@inquirer/prompts'
import fg from 'fast-glob'
import {
  basename,
  join,
} from 'pathe'
import {
  expect,
  it,
  vi,
} from 'vitest'
import { selectStartingFolder } from '../src/helpers.ts'

vi.mock('@inquirer/prompts', () => ({
  select: vi.fn(),
  Separator: vi.fn().mockImplementation(text => ({
    type: 'separator',
    line: text,
  })),
}))

const cloudFolders = fg.sync(['Library/CloudStorage/**'], {
  onlyDirectories: true,
  absolute: true,
  cwd: homedir(),
  deep: 1,
}).map(folder => ({
  name: basename(folder).replace('OneDrive-SharedLibraries', 'SharePoint-'),
  value: folder,
}))

// const homeFolders = fg.sync(['!Desktop', 'Documents', 'Downloads'], {
const homeFolders = fg.sync([join(homedir(), '/**')], {
  onlyDirectories: true,
  absolute: true,
  cwd: homedir(),
  deep: 1,
  dot: false,
  ignore: ['**/Library', '**/Applications', '**/Music', '**/Movies', '**/Pictures', '**/Public', '**/OneDrive*', '**/Reed Smith*', '**/Git*', '**/Parallels'],
}).map(folder => ({
  name: basename(folder),
  value: folder,
}))

it('should call select with correct parameters', async () => {
  const expectedChoices = [new Separator('----CURRENT----'), {
    name: basename(process.cwd()),
    value: process.cwd(),

  }, new Separator('----ONEDRIVE----'), ...cloudFolders, new Separator('----HOME----'), ...homeFolders]

  await selectStartingFolder()
  expect(select).toHaveBeenCalledWith({
    message: 'Where do you want to start looking for your undefined file?',
    pageSize: 20,
    choices: expectedChoices,
  })
})
